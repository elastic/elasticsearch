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

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

public final class FetchSourceSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.sourceRequested() == false) {
            return;
        }
        final boolean nestedHit = hitContext.hit().getNestedIdentity() != null;
        SourceLookup source = context.lookup().source();
        FetchSourceContext fetchSourceContext = context.fetchSourceContext();
        assert fetchSourceContext.fetchSource();

        // If source is disabled in the mapping, then attempt to return early.
        if (source.source() == null && source.internalSourceRef() == null) {
            if (containsFilters(fetchSourceContext)) {
                throw new IllegalArgumentException("unable to fetch fields from _source field: _source is disabled in the mappings " +
                    "for index [" + context.indexShard().shardId().getIndexName() + "]");
            }
            return;
        }

        // If this is a parent document and there are no source filters, then add the source as-is.
        if (nestedHit == false && containsFilters(fetchSourceContext) == false) {
            hitContext.hit().sourceRef(source.internalSourceRef());
            return;
        }

        // Otherwise, filter the source and add it to the hit.
        Object value = source.filter(fetchSourceContext);
        if (nestedHit) {
            value = getNestedSource((Map<String, Object>) value, hitContext);
        }

        try {
            final int initialCapacity = nestedHit ? 1024 : Math.min(1024, source.internalSourceRef().length());
            BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
            XContentBuilder builder = new XContentBuilder(source.sourceContentType().xContent(), streamOutput);
            if (value != null) {
                builder.value(value);
            } else {
                // This happens if the source filtering could not find the specified in the _source.
                // Just doing `builder.value(null)` is valid, but the xcontent validation can't detect what format
                // it is. In certain cases, for example response serialization we fail if no xcontent type can't be
                // detected. So instead we just return an empty top level object. Also this is in inline with what was
                // being return in this situation in 5.x and earlier.
                builder.startObject();
                builder.endObject();
            }
            hitContext.hit().sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new ElasticsearchException("Error filtering source", e);
        }
    }

    private static boolean containsFilters(FetchSourceContext context) {
        return context.includes().length != 0 || context.excludes().length != 0;
    }

    private Map<String, Object> getNestedSource(Map<String, Object> sourceAsMap, HitContext hitContext) {
        for (SearchHit.NestedIdentity o = hitContext.hit().getNestedIdentity(); o != null; o = o.getChild()) {
            sourceAsMap = (Map<String, Object>) sourceAsMap.get(o.getField().string());
            if (sourceAsMap == null) {
                return null;
            }
        }
        return sourceAsMap;
    }
}
