/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.xcontent.QueryParseContext;

/**
 * @author kimchy (shay.banon)
 */
public final class QueryParsers {

    private QueryParsers() {

    }

    public static Query wrapSmartNameQuery(Query query, @Nullable MapperService.SmartNameFieldMappers smartFieldMappers,
                                           QueryParseContext parseContext) {
        if (smartFieldMappers == null) {
            return query;
        }
        if (!smartFieldMappers.hasDocMapper()) {
            return query;
        }
        DocumentMapper docMapper = smartFieldMappers.docMapper();
        return new FilteredQuery(query, parseContext.cacheFilter(docMapper.typeFilter()));
    }

    public static Filter wrapSmartNameFilter(Filter filter, @Nullable MapperService.SmartNameFieldMappers smartFieldMappers,
                                             QueryParseContext parseContext) {
        if (smartFieldMappers == null) {
            return filter;
        }
        if (!smartFieldMappers.hasDocMapper()) {
            return filter;
        }
        DocumentMapper docMapper = smartFieldMappers.docMapper();
        return new AndFilter(ImmutableList.of(parseContext.cacheFilter(docMapper.typeFilter()), filter));
    }
}
