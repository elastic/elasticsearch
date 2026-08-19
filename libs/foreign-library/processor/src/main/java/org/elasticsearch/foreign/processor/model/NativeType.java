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
 * Classifies how a Java type participates in a native FFM call: which {@code ValueLayout} it
 * maps to and how it is marshaled.
 *
 * <p>Marshaling types:
 * <ul>
 *   <li>{@link #STRING}: the Java {@code String} is copied into a native {@code char *} at call
 *       time, laid out as {@link #ADDRESS}.</li>
 *   <li>{@link #ADDRESSABLE}: an {@code org.elasticsearch.foreign.Addressable} is unwrapped to
 *       the {@code long} address of its backing segment; laid out as {@link #LONG}. A
 *       {@code null} Addressable is passed as {@code 0L}.</li>
 * </ul>
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
    ADDRESSABLE,
    VOID;

    /** The native layout to use when describing this type to FFM. */
    public NativeType layoutType() {
        return switch (this) {
            case STRING -> ADDRESS;
            case ADDRESSABLE -> LONG;
            default -> this;
        };
    }
}
