/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

public class HighlighterContext {

    public final String fieldName;
    public final SearchContextHighlight.Field field;
    public final FieldMapper mapper;
    public final SearchContext context;
    public final FetchSubPhase.HitContext hitContext;
    public final Query query;

    public HighlighterContext(String fieldName, SearchContextHighlight.Field field, FieldMapper mapper, SearchContext context,
            FetchSubPhase.HitContext hitContext, Query query) {
        this.fieldName = fieldName;
        this.field = field;
        this.mapper = mapper;
        this.context = context;
        this.hitContext = hitContext;
        this.query = query;
    }
}
