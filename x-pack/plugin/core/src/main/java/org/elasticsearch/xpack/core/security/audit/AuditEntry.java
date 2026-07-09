/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

import org.elasticsearch.core.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A mutable view over the fields of a single audit log entry. Fields can be read, overwritten, or added before the entry is
 * written.
 */
public interface AuditEntry {

    /**
     * Returns the current value of the given field.
     *
     * @param field the field name
     * @return the field value, or {@code null} if the field is not set
     */
    @Nullable
    String get(String field);

    /**
     * Sets the value of the given field, overwriting any existing value.
     *
     * @param field the field name
     * @param value the value to set
     * @return this entry, to allow chaining
     */
    AuditEntry set(String field, String value);

    static AuditEntry ofFields(Map<String, String> fields) {
        var copy = new HashMap<>(fields);
        return new AuditEntry() {
            @Override
            public String get(String field) {
                return copy.get(field);
            }

            @Override
            public AuditEntry set(String field, String value) {
                copy.put(field, value);
                return this;
            }
        };
    }
}
