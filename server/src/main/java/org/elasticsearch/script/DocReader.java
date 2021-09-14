/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.Field;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Access the document in a script, provides both old-style, doc['fieldname'], and new style field('fieldname') access to the fields.
 *
 * {@code field(String)} and {@code fields(String)} may pull field contents from source as well as doc-values.  Old style access
 * only reads doc-values.
 */
public interface DocReader {
    /** New-style field access */
    Field<?> field(String fieldName);

    /** New-style field iterator */
    Stream<Field<?>> fields(String fieldGlob);

    /** Set the underlying docId */
    void setDocument(int docID);

    // Compatibility APIS
    /** Old-style doc access for contexts that map some doc contents in params */
    Map<String, Object> docAsMap();

    /** Old-style doc['field'] access */
    Map<String, ScriptDocValues<?>> doc();
}
