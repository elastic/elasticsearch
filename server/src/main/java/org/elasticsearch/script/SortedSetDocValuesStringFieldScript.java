/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class SortedSetDocValuesStringFieldScript extends StringFieldScript {
    private final SortedSetDocValues sortedSetDocValues;

    boolean hasValue = false;

    public SortedSetDocValuesStringFieldScript(String fieldName, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, Map.of(), searchLookup, OnScriptError.FAIL, ctx);
        try {
            sortedSetDocValues = DocValues.getSortedSet(ctx.reader(), fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public void setDocument(int docID) {
        try {
            hasValue = sortedSetDocValues.advanceExact(docID);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public void execute() {
        try {
            if (hasValue) {
                for (int i = 0; i < sortedSetDocValues.docValueCount(); i++) {
                    BytesRef bytesRef = sortedSetDocValues.lookupOrd(sortedSetDocValues.nextOrd());
                    emit(bytesRef.utf8ToString());
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
