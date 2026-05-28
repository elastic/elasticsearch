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

public interface AuditEntry {

    @Nullable
    String get(String field);

    AuditEntry set(String field, String value);

    /**
     * for tests
     */
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
