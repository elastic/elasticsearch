/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class SortedNumericDocValuesLongFieldScript extends AbstractLongFieldScript {

    final LongDocValuesField longDocValuesField;

    public SortedNumericDocValuesLongFieldScript(String fieldName, SearchLookup lookup, LeafReaderContext ctx) {
        super(fieldName, Map.of(), lookup, ctx);
        try {
            longDocValuesField = new LongDocValuesField(DocValues.getSortedNumeric(ctx.reader(), fieldName), fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    protected void emitFromObject(Object v) {
        // we only use doc-values, not _source, so no need to implement this method
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDocument(int docID) {
        try {
            longDocValuesField.setNextDocId(docID);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public void execute() {
        for (long value : longDocValuesField) {
            emit(value);
        }
    }
}
