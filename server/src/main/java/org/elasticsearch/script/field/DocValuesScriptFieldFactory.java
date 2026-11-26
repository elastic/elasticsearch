/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

/**
 * This interface is used to mark classes that generate
 * both {@link Field} and {@link ScriptDocValues} for use in a script.
 */
public interface DocValuesScriptFieldFactory extends ScriptFieldFactory {

    /** Set the current document ID. */
    void setNextDocId(int docId) throws IOException;

    /**
     * Returns a {@code ScriptDocValues} of the appropriate type for this field.
     * This is used to support backwards compatibility for accessing field values
     * through the {@code doc} variable.
     */
    ScriptDocValues<?> toScriptDocValues();

    /**
     * The most heap, in bytes, that this factory's underlying doc-values reader can buffer for a single decode (for
     * example a binary-block decompression buffer), or {@code -1} when there is no estimate. The script lookup uses
     * this to account for the buffer this reader retains while it is live in a circuit breaker. The default is {@code -1};
     * factories backed by a buffering binary doc-values reader override it.
     */
    default long maxDecodeBytes() {
        return -1L;
    }
}
