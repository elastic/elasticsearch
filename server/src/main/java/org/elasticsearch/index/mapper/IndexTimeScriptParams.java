/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class IndexTimeScriptParams {

    private final BytesReference source;
    private final Function<String, MappedFieldType> fieldTypeLookup;

    private LeafDocLookup docLookup;
    private SourceLookup sourceLookup;

    public IndexTimeScriptParams(BytesReference source, Function<String, MappedFieldType> fieldTypeLookup) {
        this.source = source;
        this.fieldTypeLookup = fieldTypeLookup;
    }

    public SourceLookup source() {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
        }
        return sourceLookup;
    }

    public Map<String, ScriptDocValues<?>> doc() {
        if (docLookup == null) {
            Predicate<String> fieldExists = f -> fieldTypeLookup.apply(f) != null;
            Function<String, ScriptDocValues<?>> valueLoader = f -> {
                MappedFieldType ft = fieldTypeLookup.apply(f);
                if (ft == null) {
                    throw new IllegalArgumentException("No field found for [" + f + "] in mapping");
                }
                ValueFetcher fetcher = ft.valueFetcher(null, null); // TODO source paths!
                return new SyntheticScriptDocValues(fetcher);
            };
            docLookup = new LeafDocLookup(fieldExists, valueLoader);
        }
        return docLookup;
    }

    private class SyntheticScriptDocValues extends ScriptDocValues<Object> {

        final ValueFetcher fetcher;
        List<Object> values;

        private SyntheticScriptDocValues(ValueFetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            values = fetcher.fetchValues(source());
        }

        @Override
        public Object get(int index) {
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }
    }

}
