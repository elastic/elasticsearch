/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.stream.Stream;

public class DocValuesDocReader implements DocReader, LeafReaderContextSupplier {
    /** A leaf lookup for the bound segment this proxy will operate on. */
    protected LeafSearchLookup leafLookup;

    // provide access to the leaf context reader for expressions
    protected final LeafReaderContext leafReaderContext;

    public DocValuesDocReader(SearchLookup lookup, LeafReaderContext leafContext) {
        this.leafReaderContext = leafContext;
        this.leafLookup = lookup.getLeafSearchLookup(leafReaderContext);
    }

    @Override
    public Field<?> field(String fieldName) {
        Map<String, ScriptDocValues<?>> doc = leafLookup.doc();

        if (doc.containsKey(fieldName) == false) {
            return new EmptyField<Number>(fieldName);
        }
        return new DocValuesField<>(fieldName, doc.get(fieldName));
    }


    @Override
    public Stream<Field<?>> fields(String fieldGlob) {
        return Stream.empty();
    }

    @Override
    public void setDocument(int docID) {
        leafLookup.setDocument(docID);
    }

    @Override
    public Map<String, Object> docAsMap() {
        return leafLookup.asMap();
    }

    @Override
    public Map<String, ScriptDocValues<?>> doc() {
        return leafLookup.doc();
    }

    @Override
    public LeafReaderContext getLeafReaderContext() {
        return leafReaderContext;
    }
}
