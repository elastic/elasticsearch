/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Codec-local mirror of the mapper-layer field type. The pipeline package needs to make
 * routing decisions (e.g. block size selection) based on a field's logical type, but the
 * codec layer does not import mapper classes directly. {@code PerFieldFormatSupplier}
 * translates from the concrete mapper type to this enum when building a {@link FieldContext}.
 */
public enum MappedFieldType {
    /** Maps to {@code NumberFieldMapper} types long, integer, short, and byte. */
    LONG,
    /** Maps to {@code NumberFieldMapper} type double. */
    DOUBLE,
    /** Maps to {@code NumberFieldMapper} types float and half_float. */
    FLOAT,
    /** Maps to {@code DateFieldMapper}. */
    DATE,
    /** Maps to {@code IpFieldMapper}. */
    IP
}
