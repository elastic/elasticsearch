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
 * Supplies values to different ScriptDocValues as we
 * convert them to wrappers around {@link DocValuesField}.
 * This allows for different {@link DocValuesField} to implement
 * this supplier class in many-to-one relationship since
 * {@link DocValuesField} are more specific where
 * ({byte, short, int, long, _version, murmur3, etc.} -> {long})
 */
public interface DocValuesSupplier {

    void setNextDocId(int docId) throws IOException;

    Field<?> getField(String name);

    ScriptDocValues<?> getScriptDocValues();
}
