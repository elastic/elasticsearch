/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.cache.query.parser.support;

import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.parser.QueryParserCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class AbstractJvmQueryParserCache extends AbstractIndexComponent implements QueryParserCache {

    final ConcurrentMap<QueryParserSettings, Query> cache;

    protected AbstractJvmQueryParserCache(Index index, @IndexSettings Settings indexSettings, ConcurrentMap<QueryParserSettings, Query> cache) {
        super(index, indexSettings);
        this.cache = cache;
    }

    @Override
    public void close() throws ElasticSearchException {
        clear();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public Query get(QueryParserSettings queryString) {
        return cache.get(queryString);
    }

    @Override
    public void put(QueryParserSettings queryString, Query query) {
        cache.put(queryString, query);
    }
}
