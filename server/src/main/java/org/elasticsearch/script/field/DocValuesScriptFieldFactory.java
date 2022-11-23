/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
}
