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

package org.elasticsearch.indices.cache.filter.terms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.QueryParseContext;

/**
 */
public class TermsLookup {

    private final String index;
    private final String type;
    private final String id;
    private final String routing;
    private final String path;

    @Nullable
    private final QueryParseContext queryParseContext;

    public TermsLookup(String index, String type, String id, String routing, String path, @Nullable QueryParseContext queryParseContext) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.routing = routing;
        this.path = path;
        this.queryParseContext = queryParseContext;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getRouting() {
        return this.routing;
    }

    public String getPath() {
        return path;
    }

    @Nullable
    public QueryParseContext getQueryParseContext() {
        return queryParseContext;
    }

    @Override
    public String toString() {
        return index + "/" + type + "/" + id + "/" + path;
    }
}
