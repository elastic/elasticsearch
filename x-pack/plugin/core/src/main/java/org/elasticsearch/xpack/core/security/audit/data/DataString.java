/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import java.util.Objects;

/**
 * An immutable JSON string value. The wrapped value is never {@code null}; use {@link DataNull#INSTANCE} to represent
 * the absence of a value.
 */
public record DataString(String value) implements DataValue {
    public DataString {
        Objects.requireNonNull(value, "value");
    }
}
