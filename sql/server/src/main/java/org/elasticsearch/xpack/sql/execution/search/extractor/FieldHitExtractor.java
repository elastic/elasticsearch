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
import org.elasticsearch.xpack.sql.execution.ExecutionException;
import org.joda.time.ReadableInstant;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private final String fieldName, hitName;
    private final boolean useDocValue;
    private final String[] path;

    public FieldHitExtractor(String name, boolean useDocValue) {
        this(name, useDocValue, null);
    }

    public FieldHitExtractor(String name, boolean useDocValue, String hitName) {
        this.fieldName = name;
        this.useDocValue = useDocValue;
        this.hitName = hitName;
        this.path = useDocValue ? Strings.EMPTY_ARRAY : Strings.tokenizeToStringArray(fieldName, ".");
    }

    FieldHitExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        useDocValue = in.readBoolean();
        hitName = in.readOptionalString();
        path = useDocValue ? Strings.EMPTY_ARRAY : Strings.tokenizeToStringArray(fieldName, ".");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeBoolean(useDocValue);
        out.writeOptionalString(hitName);
    }

    @Override
    public Object get(SearchHit hit) {
        Object value = null;
        if (useDocValue) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                checkMultiValue(field.getValues());
                value = field.getValue();
                if (value instanceof ReadableInstant) {
                    value = ((ReadableInstant) value).getMillis();
                }
            }
        } else {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source != null) {
                value = extractFromSource(source);
            }
        }
        return value;
    }

    private void checkMultiValue(Object values) {
        if (!ARRAYS_LENIENCY && values != null && values instanceof List && ((List<?>) values).size() > 1) {
            throw new ExecutionException("Arrays (returned by [%s]) are not supported", fieldName);
        }
    }

    @SuppressWarnings("unchecked")
    Object extractFromSource(Map<String, Object> map) {
        Object value = null;
        // each node is a key inside the map
        for (String node : path) {
            // if it's not the first step, start unpacking
            if (value != null) {
                if (value instanceof Map) {
                    map = (Map<String, Object>) value;
                } else {
                    throw new ExecutionException("Cannot extract value [%s] from source", fieldName);
                }
            }
            value = map.get(node);
        }
        checkMultiValue(value);
        return value;
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