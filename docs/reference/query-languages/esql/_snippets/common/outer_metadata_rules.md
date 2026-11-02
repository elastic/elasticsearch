Except for `_class` and `_name`, an outer `METADATA` request returns `null` when the source output does not contain the requested field.
When the source retains a metadata field in its output, the outer query receives its values even if it does not request that field with `METADATA`.
