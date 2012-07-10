/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.scriptsource;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public class ScriptSourceFetchSubPhase implements FetchSubPhase {

    private static final Map<String, Object> EMPTY_SOURCE = ImmutableMap.of();

    @Inject
    public ScriptSourceFetchSubPhase() {
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("script_source", new ScriptSourceParseElement())
                .put("scriptSource", new ScriptSourceParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.hasScriptSource();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        ScriptSourceContext script = context.scriptSource();
        script.script().setNextReader(hitContext.reader());
        script.script().setNextDocId(hitContext.docId());
        script.script().setNextSource(hitContext.sourceAsMap());
        try {
            Object value;
            value = script.script().run();
            value = script.script().unwrap(value);
            updateSource(context, hitContext, value);
        } catch (RuntimeException e) {
            if (!script.ignoreException()) {
                throw e;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateSource(SearchContext context, HitContext hitContext, Object value) {
        if (value != null) {
            if (value == context.lookup().source()) {
                // script returned _source - it means the source shouldn't be changed
                return;
            } else if (value instanceof String) {
                hitContext.source(((String)value).getBytes());
            } else if (value instanceof byte[]) {
                hitContext.source((byte[]) value);
            } else if (value instanceof Map) {
                hitContext.sourceAsMap((Map<String, Object>) value);
            } else {
                throw new ElasticSearchIllegalArgumentException("Source script returned unsupported source type " + value.getClass().getName());
            }
        } else {
            // Script returned null - no source is available
            hitContext.sourceAsMap(EMPTY_SOURCE);
        }
        // Update source lookup for other phases
        context.lookup().source().setNextSource(hitContext.sourceAsMap());
    }

}
