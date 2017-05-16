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

package org.elasticsearch.legacy.search.internal;

import org.elasticsearch.legacy.common.io.stream.StreamInput;
import org.elasticsearch.legacy.common.io.stream.StreamOutput;
import org.elasticsearch.legacy.common.io.stream.Streamable;
import org.elasticsearch.legacy.common.xcontent.ToXContent;
import org.elasticsearch.legacy.common.xcontent.XContentBuilder;
import org.elasticsearch.legacy.search.SearchHits;
import org.elasticsearch.legacy.search.aggregations.Aggregations;
import org.elasticsearch.legacy.search.aggregations.InternalAggregations;
import org.elasticsearch.legacy.search.facet.Facets;
import org.elasticsearch.legacy.search.facet.InternalFacets;
import org.elasticsearch.legacy.search.suggest.Suggest;

import java.io.IOException;

import static org.elasticsearch.legacy.search.internal.InternalSearchHits.readSearchHits;

/**
 *
 */
public class InternalSearchResponse implements Streamable, ToXContent {

    public static InternalSearchResponse empty() {
        return new InternalSearchResponse(InternalSearchHits.empty(), null, null, null, false);
    }

    private InternalSearchHits hits;

    private InternalFacets facets;

    private InternalAggregations aggregations;

    private Suggest suggest;

    private boolean timedOut;

    private InternalSearchResponse() {
    }

    public InternalSearchResponse(InternalSearchHits hits, InternalFacets facets, InternalAggregations aggregations, Suggest suggest, boolean timedOut) {
        this.hits = hits;
        this.facets = facets;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.timedOut = timedOut;
    }

    public boolean timedOut() {
        return this.timedOut;
    }

    public SearchHits hits() {
        return hits;
    }

    public Facets facets() {
        return facets;
    }

    public Aggregations aggregations() {
        return aggregations;
    }

    public Suggest suggest() {
        return suggest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (facets != null) {
            facets.toXContent(builder, params);
        }
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        if (suggest != null) {
            suggest.toXContent(builder, params);
        }
        return builder;
    }

    public static InternalSearchResponse readInternalSearchResponse(StreamInput in) throws IOException {
        InternalSearchResponse response = new InternalSearchResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        hits = readSearchHits(in);
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST, in);
        }
        timedOut = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(timedOut);
    }
}
