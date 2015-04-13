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

package org.elasticsearch.index.cache.query.parser;

import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.IndexComponent;

import java.io.Closeable;

/**
 * The main benefit of the query parser cache is to not parse the same query string on different shards.
 * Less about long running query strings.
 */
public interface QueryParserCache extends IndexComponent, Closeable {

    Query get(QueryParserSettings queryString);

    void put(QueryParserSettings queryString, Query query);

    void clear();
}
