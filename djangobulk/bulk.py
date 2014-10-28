'''
Django bulk operations on simple models.

inspired by:
https://dba.stackexchange.com/questions/13468/most-idiomatic-way-to-implement-upsert-in-postgresql-nowadays/13477#13477?s=f2acc8471f96486aaf0a5d6247241cf3
http://stackoverflow.com/questions/1109061/insert-on-duplicate-update-in-postgresql/8702291#8702291
http://www.the-art-of-web.com/sql/upsert/

'''
from itertools import repeat
from django.db import models, connections, transaction


def _model_fields(model):
    return [f for f in model._meta.fields
            if not isinstance(f, models.AutoField)]


def _prep_values(fields, obj, con):
    return tuple(f.get_db_prep_save(f.pre_save(obj, True), connection=con)
                 for f in fields)


def insert_or_update_many(model, objects, keys=None, skip_for_update=[], using="default"):
    '''
    Bulk UPSERT (UPDATE or INSERT) a list of Django objects. This uses postgres
    CTEs so it only works with postgres.

    :param model: Django model class.
    :param objects: List of objects of class `model`.
    :param keys: A list of field names to update on.
    :param skip_for_update: Fields to skip when updating (for instance created_timestamp)
    :param using: Database to use.

    '''
    if not objects:
        return

    keys = keys or [model._meta.pk.name]
    con = connections[using]

    table = model._meta.db_table

    all_fields = _model_fields(model)

    # these are the fields that will be INSERTed on a failed UPDATE
    all_field_names = [f.name for f in all_fields]
    all_col_names = ",".join(con.ops.quote_name(f.column) for f in all_fields)

    # key fields are those used for WHERE in the UPDATE
    key_fields = [f for f in model._meta.fields if f.name in keys and f.name in all_field_names]
    key_col_names = ",".join(con.ops.quote_name(f.column) for f in key_fields)

    # Select key tuples from the database to find out which ones need to be
    # updated and which ones need to be inserted.
    assert key_fields, "Empty key fields"

    # update fields are those whose values are updated
    update_fields = [f for f in model._meta.fields if f.name not in keys and f.name not in skip_for_update and f.name in all_field_names]
    update_col_names = ",".join(con.ops.quote_name(f.column) for f in update_fields)

    # repeat tuple values
    # tuple_placeholder = "(%s)" % ",".join(repeat("%s", len(all_fields)))
    tuple_placeholder = "(%s)" % ",".join("%%s::%s" % f.db_type(con) for f in all_fields)  # TODO: the type decoration is only necessary for the first row...
    placeholders = ",".join(repeat(tuple_placeholder, len(objects)))

    parameters = [_prep_values(all_fields, o, con) for o in objects]
    parameters = [field for row in parameters for field in row]  # TODO: de-brainfuck this

    assignments = ",".join("%(f)s=nv.%(f)s" % {
        'f': con.ops.quote_name(f.column)
    } for f in update_fields)

    where_keys = " AND ".join("m.%(f)s=nv.%(f)s" % {
        'f': con.ops.quote_name(f.column)
    } for f in key_fields)

    up_where_keys = " AND ".join("up.%(f)s=new_values.%(f)s" % {
        'f': con.ops.quote_name(f.column)
    } for f in key_fields)

    sql_replacements = dict(
        keys=keys,
        table=table,
        all_fields=all_fields,
        all_col_names=all_col_names,
        key_fields=key_fields,
        key_col_names=key_col_names,
        update_fields=update_fields,
        update_col_names=update_col_names,
        tuple_placeholder=tuple_placeholder,
        placeholders=placeholders,
        parameters=parameters,
        assignments=assignments,
        where_keys=where_keys,
        up_where_keys=up_where_keys,
    )

    # return sql_replacements  # DEBUG

    sql = """
        WITH new_values (%(all_col_names)s) AS (
          VALUES
            %(placeholders)s
        ),
        upsert AS
        (
            UPDATE %(table)s m
                SET %(assignments)s
            FROM new_values nv
            WHERE %(where_keys)s
            RETURNING m.*
        )
        INSERT INTO %(table)s (%(all_col_names)s)
        SELECT %(all_col_names)s
        FROM new_values
        WHERE NOT EXISTS (SELECT 1
                          FROM upsert up
                          WHERE %(up_where_keys)s)
    """ % sql_replacements

    # return sql, parameters  # DEBUG

    cursor = con.cursor()
    cursor.execute(sql, parameters)

    transaction.commit_unless_managed()
