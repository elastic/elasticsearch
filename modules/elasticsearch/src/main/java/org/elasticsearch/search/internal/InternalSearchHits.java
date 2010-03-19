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

package org.elasticsearch.search.internal;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

import static org.elasticsearch.search.internal.InternalSearchHit.*;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalSearchHits implements SearchHits {

    private static final SearchHit[] EMPTY = new SearchHit[0];

    private SearchHit[] hits;

    private long totalHits;

    private InternalSearchHits() {

    }

    public InternalSearchHits(SearchHit[] hits, long totalHits) {
        this.hits = hits;
        this.totalHits = totalHits;
    }

    public long totalHits() {
        return totalHits;
    }

    public SearchHit[] hits() {
        return this.hits;
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject("hits");
        builder.field("total", totalHits);
        builder.field("hits");
        builder.startArray();
        for (SearchHit hit : hits) {
            hit.toJson(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    public static InternalSearchHits readSearchHits(StreamInput in) throws IOException {
        InternalSearchHits hits = new InternalSearchHits();
        hits.readFrom(in);
        return hits;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        totalHits = in.readVLong();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = readSearchHit(in);
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHits);
        out.writeVInt(hits.length);
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
    }
}