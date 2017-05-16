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

package org.elasticsearch.legacy.search.fetch.source;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.xcontent.XContentBuilder;
import org.elasticsearch.legacy.common.xcontent.XContentFactory;
import org.elasticsearch.legacy.search.SearchParseElement;
import org.elasticsearch.legacy.search.fetch.FetchSubPhase;
import org.elasticsearch.legacy.search.internal.InternalSearchHit;
import org.elasticsearch.legacy.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 */
public class FetchSourceSubPhase implements FetchSubPhase {

    @Inject
    public FetchSourceSubPhase() {

    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("_source", new FetchSourceParseElement());
        return parseElements.build();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticsearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.sourceRequested();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticsearchException {
        FetchSourceContext fetchSourceContext = context.fetchSourceContext();
        assert fetchSourceContext.fetchSource();
        if (fetchSourceContext.includes().length == 0 && fetchSourceContext.excludes().length == 0) {
            hitContext.hit().sourceRef(context.lookup().source().internalSourceRef());
            return;
        }

        Object value = context.lookup().source().filter(fetchSourceContext.includes(), fetchSourceContext.excludes());
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(context.lookup().source().sourceContentType());
            builder.value(value);
            hitContext.hit().sourceRef(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("Error filtering source", e);
        }

    }
}
