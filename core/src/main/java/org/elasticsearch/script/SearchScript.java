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
package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A search script.
 */
public interface SearchScript {

    LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException;

    /**
     * Indicates if document scores may be needed by this {@link SearchScript}.
     * 
     * @return {@code true} if scores are needed.
     */
    boolean needsScores();

    interface Compiled {
        SearchScript newInstance(Map<String, Object> params, SearchLookup lookup);
    }

    ScriptContext<Compiled> CONTEXT = new ScriptContext<>("search", Compiled.class);
    // TODO: remove aggs context when it has its own interface
    ScriptContext<SearchScript.Compiled> AGGS_CONTEXT = new ScriptContext<>("aggs", Compiled.class);
}