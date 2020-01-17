/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class TimeField extends AbstractField {

    static final String TYPE = "date";

    private static final Set<String> TYPES = Collections.singleton(TYPE);

    private static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

    private final Method method;

    public TimeField(String name, Method method) {
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
            // Since nanosecond support was added epoch_millis timestamps may have a fractional component.
            // We discard this, taking just whole milliseconds.  Arguably it would be better to retain the
            // precision here and let the downstream component decide whether it wants the accuracy, but
            // that makes it hard to pass around the value as a number.  The double type doesn't have
            // enough digits of accuracy, and obviously long cannot store the fraction.  BigDecimal would
            // work, but that isn't supported by the JSON parser if the number gets round-tripped through
            // JSON.  So String is really the only format that could be used, but the ML consumers of time
            // are expecting a number.
            value[0] = Long.parseLong(((String) value[0]).replaceFirst("\\..*", ""));
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
