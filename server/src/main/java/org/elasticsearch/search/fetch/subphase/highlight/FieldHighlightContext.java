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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase;

public class FieldHighlightContext {

    public final String fieldName;
    public final SearchHighlightContext.Field field;
    public final MappedFieldType fieldType;
    public final SearchShardTarget shardTarget;
    public final QueryShardContext context;
    public final FetchSubPhase.HitContext hitContext;
    public final Query query;
    public final boolean forceSource;

    public FieldHighlightContext(String fieldName,
                                 SearchHighlightContext.Field field,
                                 MappedFieldType fieldType,
                                 SearchShardTarget shardTarget,
                                 QueryShardContext context,
                                 FetchSubPhase.HitContext hitContext,
                                 Query query,
                                 boolean forceSource) {
        this.fieldName = fieldName;
        this.field = field;
        this.fieldType = fieldType;
        this.shardTarget = shardTarget;
        this.context = context;
        this.hitContext = hitContext;
        this.query = query;
        this.forceSource = forceSource;
    }
}
