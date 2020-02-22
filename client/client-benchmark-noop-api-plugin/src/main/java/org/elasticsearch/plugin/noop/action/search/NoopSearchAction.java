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
package org.elasticsearch.plugin.noop.action.search;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;

public class NoopSearchAction extends ActionType<SearchResponse> {
    public static final NoopSearchAction INSTANCE = new NoopSearchAction();
    public static final String NAME = "mock:data/read/search";

    private NoopSearchAction() {
        super(NAME, SearchResponse::new);
    }
}
