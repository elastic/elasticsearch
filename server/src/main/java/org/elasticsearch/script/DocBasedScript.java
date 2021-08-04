/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public abstract class DocBasedScript {
    protected final DocReader docReader;

    public DocBasedScript(DocReader docReader) {
        this.docReader = docReader;
    }

    public Field<?> field(String fieldName) {
        if (docReader == null) {
            return new EmptyField<>(fieldName);
        }
        return docReader.field(fieldName);
    }

    public Stream<Field<?>> fields(String fieldGlob) {
        if (docReader == null) {
            return Stream.empty();
        }
        return docReader.fields(fieldGlob);
    }

    /**
     * Set the current document to run the script on next.
     */
    public void setDocument(int docID) {
        if (docReader != null) {
            docReader.setDocument(docID);
        }
    }

    public Map<String, Object> docAsMap() {
        if (docReader == null) {
            return Collections.emptyMap();
        }
        return docReader.docAsMap();
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        if (docReader == null) {
            return Collections.emptyMap();
        }
        return docReader.doc();
    }
}
