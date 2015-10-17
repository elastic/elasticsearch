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

package org.elasticsearch.search.fetch.source;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 */
public class FetchSourceSubPhase implements FetchSubPhase {
    private static final Map<String, SearchParseElement> PARSE_ELEMENTS = singletonMap("_source", new FetchSourceParseElement());

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return PARSE_ELEMENTS;
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.sourceRequested();
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        FetchSourceContext fetchSourceContext = context.fetchSourceContext();
        assert fetchSourceContext.fetchSource();
        if (fetchSourceContext.includes().length == 0 && fetchSourceContext.excludes().length == 0) {
            hitContext.hit().sourceRef(context.lookup().source().internalSourceRef());
            return;
        }

        SourceLookup source = context.lookup().source();
        Object value = source.filter(fetchSourceContext.includes(), fetchSourceContext.excludes());
        try {
            final int initialCapacity = Math.min(1024, source.internalSourceRef().length());
            BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
            XContentBuilder builder = new XContentBuilder(context.lookup().source().sourceContentType().xContent(), streamOutput);
            builder.value(value);
            hitContext.hit().sourceRef(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("Error filtering source", e);
        }

    }
}
