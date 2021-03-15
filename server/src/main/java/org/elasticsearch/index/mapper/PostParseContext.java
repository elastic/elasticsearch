/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.lookup.SearchLookup;

public class PostParseContext {

    public final SearchLookup searchLookup;
    public final LeafReaderContext leafReaderContext;
    public final ParseContext pc;

    public PostParseContext(MappingLookup mappingLookup, ParseContext pc, LeafReaderContext ctx) {
        this.searchLookup = new SearchLookup(
            mappingLookup::getFieldType,
            (ft, s) -> ft.fielddataBuilder(pc.indexSettings().getIndex().getName(), s).build(
                new IndexFieldDataCache.None(),
                new NoneCircuitBreakerService())
        );
        this.pc = pc;
        this.leafReaderContext = ctx;
    }

}
