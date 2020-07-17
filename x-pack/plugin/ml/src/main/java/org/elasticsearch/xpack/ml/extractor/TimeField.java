/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class TimeField extends AbstractField {

    static final Set<String> TYPES = Collections.unmodifiableSet(Sets.newHashSet("date", "date_nanos"));

    private static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

    private final Method method;

    public TimeField(String name, Method method) {
        // This class intentionally reports the possible types rather than the types reported by
        // field caps at the point of construction.  This means that it will continue to work if,
        // for example, a newly created index has a "date_nanos" time field when in all the indices
        // that matched the pattern when this constructor was called the field had type "date".
        super(name, TYPES);
        if (method == Method.SOURCE) {
            throw new IllegalArgumentException("time field [" + name + "] cannot be extracted from source");
        }
        this.method = Objects.requireNonNull(method);
    }

    @Override
    public Method getMethod() {
        return method;
    }

    @Override
    public Object[] value(SearchHit hit) {
        Object[] value = getFieldValue(hit);
        if (value.length != 1) {
            return value;
        }
        if (value[0] instanceof String) { // doc_value field with the epoch_millis format
            value[0] = TimeUtils.parseToEpochMs((String)value[0]);
        } else if (value[0] instanceof Long == false) { // pre-6.0 field
            throw new IllegalStateException("Unexpected value for a time field: " + value[0].getClass());
        }
        return value;
    }

    @Override
    public String getDocValueFormat() {
        if (method != Method.DOC_VALUE) {
            throw new UnsupportedOperationException();
        }
        return EPOCH_MILLIS_FORMAT;
    }

    @Override
    public boolean supportsFromSource() {
        return false;
    }

    @Override
    public ExtractedField newFromSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMultiField() {
        return false;
    }
}
