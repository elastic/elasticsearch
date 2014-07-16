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

import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * A search script.
 *
 * @see {@link ExplainableSearchScript} for script which can explain a score
 */
public interface SearchScript extends ExecutableScript, ReaderContextAware, ScorerAware {

    void setNextDocId(int doc);

    void setNextSource(Map<String, Object> source);

    void setNextScore(float score);

    float runAsFloat();

    long runAsLong();

    double runAsDouble();

    public static class Builder {

        private String script;
        private ScriptService.ScriptType scriptType;
        private String lang;
        private Map<String, Object> params;

        public Builder script(String script, ScriptService.ScriptType scriptType) {
            this.script = script;
            this.scriptType = scriptType;
            return this;
        }

        public Builder lang(String lang) {
            this.lang = lang;
            return this;
        }

        public Builder params(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public SearchScript build(SearchContext context) {
            return build(context.scriptService(), context.lookup());
        }

        public SearchScript build(ScriptService service, SearchLookup lookup) {
            return service.search(lookup, lang, script, scriptType, params);
        }
    }
}