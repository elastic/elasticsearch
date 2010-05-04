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
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.util.collect.Iterators;
import org.elasticsearch.util.gnu.trove.TIntObjectHashMap;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.search.SearchShardTarget.*;
import static org.elasticsearch.search.internal.InternalSearchHit.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalSearchHits implements SearchHits {

    private static final InternalSearchHit[] EMPTY = new InternalSearchHit[0];

    private InternalSearchHit[] hits;

    private long totalHits;

    InternalSearchHits() {

    }

    public InternalSearchHits(InternalSearchHit[] hits, long totalHits) {
        this.hits = hits;
        this.totalHits = totalHits;
    }

    public long totalHits() {
        return totalHits;
    }

    @Override public long getTotalHits() {
        return totalHits();
    }

    public SearchHit[] hits() {
        return this.hits;
    }

    @Override public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override public SearchHit[] getHits() {
        return hits();
    }

    @Override public Iterator<SearchHit> iterator() {
        return Iterators.forArray(hits());
    }

    public InternalSearchHit[] internalHits() {
        return this.hits;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("hits");
        builder.field("total", totalHits);
        builder.field("hits");
        builder.startArray();
        for (SearchHit hit : hits) {
            hit.toXContent(builder, params);
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
            // read the lookup table first
            int lookupSize = in.readVInt();
            TIntObjectHashMap<SearchShardTarget> shardLookupMap = null;
            if (lookupSize > 0) {
                shardLookupMap = new TIntObjectHashMap<SearchShardTarget>(lookupSize);
                for (int i = 0; i < lookupSize; i++) {
                    shardLookupMap.put(in.readVInt(), readSearchShardTarget(in));
                }
            }

            hits = new InternalSearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = readSearchHit(in, shardLookupMap);
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHits);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            // write the header search shard targets (we assume identity equality)
            IdentityHashMap<SearchShardTarget, Integer> shardLookupMap = new IdentityHashMap<SearchShardTarget, Integer>();
            // start from 1, 0 is for null!
            int counter = 1;
            // put an entry for null
            for (InternalSearchHit hit : hits) {
                if (hit.shard() != null) {
                    Integer handle = shardLookupMap.get(hit.shard());
                    if (handle == null) {
                        shardLookupMap.put(hit.shard(), counter++);
                    }
                }
            }
            out.writeVInt(shardLookupMap.size());
            if (!shardLookupMap.isEmpty()) {
                for (Map.Entry<SearchShardTarget, Integer> entry : shardLookupMap.entrySet()) {
                    out.writeVInt(entry.getValue());
                    entry.getKey().writeTo(out);
                }
            }

            for (InternalSearchHit hit : hits) {
                hit.writeTo(out, shardLookupMap.isEmpty() ? null : shardLookupMap);
            }
        }
    }
}