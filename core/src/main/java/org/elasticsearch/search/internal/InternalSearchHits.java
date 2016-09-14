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

package org.elasticsearch.search.internal;

import com.carrotsearch.hppc.IntObjectHashMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.search.internal.InternalSearchHit.readSearchHit;

/**
 *
 */
public class InternalSearchHits implements SearchHits {

    public static InternalSearchHits empty() {
        // We shouldn't use static final instance, since that could directly be returned by native transport clients
        return new InternalSearchHits(EMPTY, 0, 0);
    }

    public static final InternalSearchHit[] EMPTY = new InternalSearchHit[0];

    private InternalSearchHit[] hits;

    public long totalHits;

    private float maxScore;

    InternalSearchHits() {

    }

    public InternalSearchHits(InternalSearchHit[] hits, long totalHits, float maxScore) {
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
    }

    public void shardTarget(SearchShardTarget shardTarget) {
        for (InternalSearchHit hit : hits) {
            hit.shard(shardTarget);
        }
    }

    @Override
    public long totalHits() {
        return totalHits;
    }

    @Override
    public long getTotalHits() {
        return totalHits();
    }

    @Override
    public float maxScore() {
        return this.maxScore;
    }

    @Override
    public float getMaxScore() {
        return maxScore();
    }

    @Override
    public SearchHit[] hits() {
        return this.hits;
    }

    @Override
    public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override
    public SearchHit[] getHits() {
        return hits();
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.stream(hits()).iterator();
    }

    public InternalSearchHit[] internalHits() {
        return this.hits;
    }

    static final class Fields {
        static final String HITS = "hits";
        static final String TOTAL = "total";
        static final String MAX_SCORE = "max_score";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HITS);
        builder.field(Fields.TOTAL, totalHits);
        if (Float.isNaN(maxScore)) {
            builder.nullField(Fields.MAX_SCORE);
        } else {
            builder.field(Fields.MAX_SCORE, maxScore);
        }
        builder.field(Fields.HITS);
        builder.startArray();
        for (SearchHit hit : hits) {
            hit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }


    public static InternalSearchHits readSearchHits(StreamInput in) throws IOException {
        InternalSearchHits hits = new InternalSearchHits();
        hits.readFrom(in);
        return hits;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalHits = in.readVLong();
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new InternalSearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = readSearchHit(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHits);
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (InternalSearchHit hit : hits) {
                hit.writeTo(out);
            }
        }
    }
}
