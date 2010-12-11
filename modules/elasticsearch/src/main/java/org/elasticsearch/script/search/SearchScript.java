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

package org.elasticsearch.script.search;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.lookup.SearchLookup;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class SearchScript {

    private final SearchLookup searchLookup;

    private final ExecutableScript script;

    public SearchScript(SearchLookup searchLookup, ExecutableScript script) {
        this.searchLookup = searchLookup;
        this.script = script;
    }

    public SearchScript(SearchLookup searchLookup, String lang, String script, @Nullable Map<String, Object> params, ScriptService scriptService) {
        this.searchLookup = searchLookup;
        this.script = scriptService.executable(lang, script, searchLookup.processAsMap(params));
    }

    public SearchScript(String lang, String script, @Nullable Map<String, Object> params, ScriptService scriptService, MapperService mapperService, FieldDataCache fieldDataCache) {
        this.searchLookup = new SearchLookup(mapperService, fieldDataCache);
        this.script = scriptService.executable(lang, script, searchLookup.processAsMap(params));
    }

    public void setNextReader(IndexReader reader) {
        searchLookup.setNextReader(reader);
    }

    public Object execute(int docId) {
        searchLookup.setNextDocId(docId);
        return script.run();
    }

    public Object execute(int docId, Map params) {
        searchLookup.setNextDocId(docId);
        return script.run(params);
    }
}
