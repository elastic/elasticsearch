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

package org.elasticsearch.cache.query.parser.support;

import org.apache.lucene.queryParser.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.elasticsearch.cache.query.parser.QueryParserCache;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class AbstractJvmQueryParserCache extends AbstractComponent implements QueryParserCache {

    final ConcurrentMap<QueryParserSettings, Query> cache;

    protected AbstractJvmQueryParserCache(Settings settings, ConcurrentMap<QueryParserSettings, Query> cache) {
        super(settings);
        this.cache = cache;
    }

    @Override public void clear() {
        cache.clear();
    }

    @Override public Query get(QueryParserSettings queryString) {
        return cache.get(queryString);
    }

    @Override public void put(QueryParserSettings queryString, Query query) {
        cache.put(queryString, query);
    }
}
