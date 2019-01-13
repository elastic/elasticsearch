/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public class FieldHitExtractor implements HitExtractor {

    private static final boolean ARRAYS_LENIENCY = false;

    /**
     * Stands for {@code field}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
    static final String NAME = "f";

    /**
     * Source extraction requires only the (relative) field name, without its parent path.
     */
    private static String[] sourcePath(String name, boolean useDocValue, String hitName) {
        return useDocValue ? Strings.EMPTY_ARRAY : Strings
                .tokenizeToStringArray(hitName == null ? name : name.substring(hitName.length() + 1), ".");
    }

    private final String fieldName, hitName;
    private final DataType dataType;
    private final boolean useDocValue;
    private final String[] path;

    public FieldHitExtractor(String name, DataType dataType, boolean useDocValue) {
        this(name, dataType, useDocValue, null);
    }

    public FieldHitExtractor(String name, DataType dataType, boolean useDocValue, String hitName) {
        this.fieldName = name;
        this.dataType = dataType;
        this.useDocValue = useDocValue;
        this.hitName = hitName;

        if (hitName != null) {
            if (!name.contains(hitName)) {
                throw new SqlIllegalArgumentException("Hitname [{}] specified but not part of the name [{}]", hitName, name);
            }
        }

        this.path = sourcePath(fieldName, useDocValue, hitName);
    }

    FieldHitExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        String esType = in.readOptionalString();
        dataType = esType != null ? DataType.fromTypeName(esType) : null;
        useDocValue = in.readBoolean();
        hitName = in.readOptionalString();
        path = sourcePath(fieldName, useDocValue, hitName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalString(dataType == null ? null : dataType.esType);
        out.writeBoolean(useDocValue);
        out.writeOptionalString(hitName);
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        if (useDocValue) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                value = unwrapMultiValue(field.getValues());
            }
        } else {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source != null) {
                value = extractFromSource(source);
            }
        }
        return value;
    }

    private Object unwrapMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (list.isEmpty()) {
                return null;
            } else {
                if (ARRAYS_LENIENCY || list.size() == 1) {
                    return unwrapMultiValue(list.get(0));
                } else {
                    throw new SqlIllegalArgumentException("Arrays (returned by [{}]) are not supported", fieldName);
                }
            }
        }
        if (values instanceof Map) {
            throw new SqlIllegalArgumentException("Objects (returned by [{}]) are not supported", fieldName);
        }
        if (dataType == DataType.DATE) {
            if (values instanceof String) {
                return DateUtils.of(Long.parseLong(values.toString()));
            }
            // returned by nested types...
            if (values instanceof DateTime) {
                return DateUtils.of((DateTime) values);
            }
        }
        if (values instanceof Long || values instanceof Double || values instanceof String || values instanceof Boolean) {
            return values;
        }
        throw new SqlIllegalArgumentException("Type {} (returned by [{}]) is not supported", values.getClass().getSimpleName(), fieldName);
    }

    @SuppressWarnings("unchecked")
    Object extractFromSource(Map<String, Object> map) {
        Object value = null;

        for (int i = 0; i < path.length; i++) {
            // each node is a key inside the map
            value = map.get(path[i]);

            // If value is not found or the map has multiple entries with common prefix
            // (e.g.: <"a","value">, <"a.b", "value">) try to extract field with dots (e.g.: "a.b")
            if (value == null || map.size() > 1) {
                StringJoiner sj = new StringJoiner(".");
                sj.add(path[i]);
                int valueFoundIdx = i;
                // Try to find the longest path in the map, e.g.:
                // for <"a.b", "value">, <"a.b.c", "value">) extract the latter.
                for (int j = i + 1; j < path.length; j++) {
                    sj.add(path[j]);
                    Object v = map.get(sj.toString());
                    if (v != null) {
                        value = v;
                        valueFoundIdx = j;
                    }
                }
                // If it is a nested object advance `i` to the current index in the `path`.
                // e.g.: if the hierarchy is {"a" : { "b.c" : { "d" : "value" }}} then i was
                // previously 0 and it becomes 2 since "a" and "b.c" are consumed.
                i = valueFoundIdx;
                if (value instanceof Map) {
                    map = (Map<String, Object>) value;
                }
            } else if (value instanceof Map) {
                map = (Map<String, Object>) value;
            } else if (i < path.length - 1) {
                // If we reach a concrete value without exhausting the full path, something is wrong with the mapping
                // e.g.: map is {"a" : { "b" : "value }} and we are looking for a path: "a.b.c.d"
                throw new SqlIllegalArgumentException("Cannot extract value [{}] from source", fieldName);
            }
        }
        return unwrapMultiValue(value);
    }

    @Override
    public String hitName() {
        return hitName;
    }

    public String fieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return fieldName + "@" + hitName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        FieldHitExtractor other = (FieldHitExtractor) obj;
        return fieldName.equals(other.fieldName)
                && hitName.equals(other.hitName)
                && useDocValue == other.useDocValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, useDocValue, hitName);
    }
}
