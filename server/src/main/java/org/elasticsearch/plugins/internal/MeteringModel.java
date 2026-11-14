/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

/**
 * The accounting an {@link XContentMeteringParserDecorator} applies to the value tokens of a document, expressed per value so that
 * it can be applied without parsing: a columnar batch is metered in one sweep over its columns instead of one parser walk per row.
 * Only values are charged, never field names, objects or arrays, and a root field is either charged or excluded as a whole, so the
 * amount does not depend on how a document is nested or whether a name is written with dots.
 */
public interface MeteringModel {

    /**
     * Whether the values of the leaf field {@code fieldName}, given by its full dotted name, are left out because its root field is
     * excluded; evaluated once per column of a batch.
     */
    boolean hasExcludedRootField(String fieldName);

    /** The size charged for a string value of {@code utf8Length} bytes. */
    long stringBytes(int utf8Length);

    /** The size charged for a binary value of {@code length} bytes. */
    long binaryBytes(int length);

    /** The size charged for a number, whatever its width. */
    long numberBytes();

    /** The size charged for a boolean. */
    long booleanBytes();
}
