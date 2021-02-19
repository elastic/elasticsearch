/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.function.Function;
import java.util.function.Supplier;

public class IndexTimeScriptParams {

    private final BytesReference source;
    private final ParseContext.Document document;
    private final Function<String, MappedFieldType> fieldTypeLookup;

    private SourceLookup sourceLookup;
    private LeafDocLookup docLookup;

    public IndexTimeScriptParams(BytesReference source, ParseContext.Document document, Function<String, MappedFieldType> fieldTypeLookup) {
        this.source = source;
        this.document = document;
        this.fieldTypeLookup = fieldTypeLookup;
    }

    public IndexTimeScriptParams(ParseContext context) {
        this(context.sourceToParse().source(), context.rootDoc(), context.mappingLookup()::getFieldType);
    }

    public SourceLookup source() {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
        }
        return sourceLookup;
    }

    public LeafDocLookup doc() {
        if (docLookup == null) {
            LazyDocReader reader = new LazyDocReader();
            docLookup = new LeafDocLookup(fieldTypeLookup, ft -> {
                IndexFieldData.Builder builder = ft.fielddataBuilder("", () -> null); // TODO sig
                return builder.build(EMPTY_CACHE, NO_BREAKER).load(reader.get()).getScriptValues();
            });
        }
        return docLookup;
    }

    private static final IndexFieldDataCache EMPTY_CACHE = new IndexFieldDataCache.None();
    private static final CircuitBreakerService NO_BREAKER = new NoneCircuitBreakerService();

    private class LazyDocReader implements Supplier<LeafReaderContext> {
        private LeafReaderContext ctx;

        @Override
        public LeafReaderContext get() {
            if (ctx == null) {
                ctx = buildMemoryIndex();
            }
            return ctx;
        }

        private LeafReaderContext buildMemoryIndex() {
            MemoryIndex mi = new MemoryIndex();
            for (IndexableField f : document.getFields()) {
                if (f.fieldType().docValuesType() != null) {
                    mi.addField(f, null);
                }
            }
            return mi.createSearcher().getIndexReader().leaves().get(0);
        }
    }
}
