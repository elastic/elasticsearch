/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.util.Map;

public class FieldHighlightContext {

    public final String fieldName;
    public final SearchHighlightContext.Field field;
    public final MappedFieldType fieldType;
    public final FetchContext context;
    public final FetchSubPhase.HitContext hitContext;
    public final Query query;
    public final boolean forceSource;
    public final Map<String, Object> cache;

    public FieldHighlightContext(String fieldName,
                                 SearchHighlightContext.Field field,
                                 MappedFieldType fieldType,
                                 FetchContext context,
                                 FetchSubPhase.HitContext hitContext,
                                 Query query,
                                 boolean forceSource,
                                 Map<String, Object> cache) {
        this.fieldName = fieldName;
        this.field = field;
        this.fieldType = fieldType;
        this.context = context;
        this.hitContext = hitContext;
        this.query = query;
        this.forceSource = forceSource;
        this.cache = cache;
    }
}
