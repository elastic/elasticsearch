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

package org.elasticsearch.search.fetch;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * Shard level fetch request used with search. Holds indices taken from the original search request
 * and implements {@link org.elasticsearch.action.IndicesRequest}.
 */
public class ShardFetchSearchRequest extends ShardFetchRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;
    private final RescoreDocIds rescoreDocIds;
    private final AggregatedDfs aggregatedDfs;

    public ShardFetchSearchRequest(OriginalIndices originalIndices, ShardSearchContextId id, ShardSearchRequest shardSearchRequest,
                                   IntArrayList list, ScoreDoc lastEmittedDoc, RescoreDocIds rescoreDocIds, AggregatedDfs aggregatedDfs) {
        super(id, list, lastEmittedDoc);
        this.originalIndices = originalIndices;
        this.shardSearchRequest = shardSearchRequest;
        this.rescoreDocIds = rescoreDocIds;
        this.aggregatedDfs = aggregatedDfs;
    }

    public ShardFetchSearchRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
            rescoreDocIds = new RescoreDocIds(in);
            aggregatedDfs = in.readOptionalWriteable(AggregatedDfs::new);
        } else {
            shardSearchRequest = null;
            rescoreDocIds = RescoreDocIds.EMPTY;
            aggregatedDfs = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(shardSearchRequest);
            rescoreDocIds.writeTo(out);
            out.writeOptionalWriteable(aggregatedDfs);
        }
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    @Override
    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    @Override
    public AggregatedDfs getAggregatedDfs() {
        return aggregatedDfs;
    }
}
