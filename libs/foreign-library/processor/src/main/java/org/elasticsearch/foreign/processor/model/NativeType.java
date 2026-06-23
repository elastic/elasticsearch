/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

/**
 * Classifies how a Java type participates in a native FFM call: which
 * {@code ValueLayout} it maps to and how it is marshaled. {@link #STRING} is the only type that
 * requires marshaling — the Java {@code String} is passed through native code as a {@code char *},
 * laid out as {@link #ADDRESS}.
 */
public enum NativeType {
    INT,
    LONG,
    SHORT,
    BYTE,
    BOOLEAN,
    FLOAT,
    DOUBLE,
    ADDRESS,
    STRING,
    VOID;

    /** The native layout to use when describing this type to FFM. {@link #STRING} is laid out as {@link #ADDRESS}. */
    public NativeType layoutType() {
        return this == STRING ? ADDRESS : this;
    }
}
