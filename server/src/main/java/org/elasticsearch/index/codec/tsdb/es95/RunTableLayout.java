/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

/**
 * The discriminator byte that {@link RunTableSortedCodec} and {@link RunTableSortedSetCodec} write at the
 * start of every field's ordinal metadata, saying which layout follows. A future change to a run-table
 * layout can take a new value instead of a new format.
 */
final class RunTableLayout {

    /** The fallback codec wrote this field's ordinal stream. */
    static final byte LAYOUT_DEFAULT = 0;
    /** The run-table layout follows. */
    static final byte LAYOUT_RUN_TABLE = 1;

    private RunTableLayout() {}
}
