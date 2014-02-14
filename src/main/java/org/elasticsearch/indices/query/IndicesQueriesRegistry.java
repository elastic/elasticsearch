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

package org.elasticsearch.indices.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParser;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class IndicesQueriesRegistry extends AbstractComponent {

    private ImmutableMap<String, QueryParser> queryParsers;
    private ImmutableMap<String, FilterParser> filterParsers;

    @Inject
    public IndicesQueriesRegistry(Settings settings, Set<QueryParser> injectedQueryParsers, Set<FilterParser> injectedFilterParsers) {
        super(settings);
        Map<String, QueryParser> queryParsers = Maps.newHashMap();
        for (QueryParser queryParser : injectedQueryParsers) {
            addQueryParser(queryParsers, queryParser);
        }
        this.queryParsers = ImmutableMap.copyOf(queryParsers);

        Map<String, FilterParser> filterParsers = Maps.newHashMap();
        for (FilterParser filterParser : injectedFilterParsers) {
            addFilterParser(filterParsers, filterParser);
        }
        this.filterParsers = ImmutableMap.copyOf(filterParsers);
    }

    /**
     * Adds a global query parser.
     */
    public synchronized void addQueryParser(QueryParser queryParser) {
        Map<String, QueryParser> queryParsers = Maps.newHashMap(this.queryParsers);
        addQueryParser(queryParsers, queryParser);
        this.queryParsers = ImmutableMap.copyOf(queryParsers);
    }

    public synchronized void addFilterParser(FilterParser filterParser) {
        Map<String, FilterParser> filterParsers = Maps.newHashMap(this.filterParsers);
        addFilterParser(filterParsers, filterParser);
        this.filterParsers = ImmutableMap.copyOf(filterParsers);
    }

    public ImmutableMap<String, QueryParser> queryParsers() {
        return queryParsers;
    }

    public ImmutableMap<String, FilterParser> filterParsers() {
        return filterParsers;
    }

    private void addQueryParser(Map<String, QueryParser> queryParsers, QueryParser queryParser) {
        for (String name : queryParser.names()) {
            queryParsers.put(name, queryParser);
        }
    }

    private void addFilterParser(Map<String, FilterParser> filterParsers, FilterParser filterParser) {
        for (String name : filterParser.names()) {
            filterParsers.put(name, filterParser);
        }
    }
}