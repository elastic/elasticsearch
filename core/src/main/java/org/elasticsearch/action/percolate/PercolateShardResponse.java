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
package org.elasticsearch.action.percolate;

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolateContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class PercolateShardResponse extends BroadcastShardResponse {

    private TopDocs topDocs;
    private Map<Integer, String> ids;
    private Map<Integer, Map<String, HighlightField>> hls;
    private boolean onlyCount;
    private int requestedSize;

    private InternalAggregations aggregations;
    private List<SiblingPipelineAggregator> pipelineAggregators;

    PercolateShardResponse() {
    }

    public PercolateShardResponse(TopDocs topDocs, Map<Integer, String> ids, Map<Integer, Map<String, HighlightField>> hls, PercolateContext context) {
        super(new ShardId(context.shardTarget().getIndex(), context.shardTarget().getShardId()));
        this.topDocs = topDocs;
        this.ids = ids;
        this.hls = hls;
        this.onlyCount = context.isOnlyCount();
        this.requestedSize = context.size();
        QuerySearchResult result = context.queryResult();
        if (result != null) {
            if (result.aggregations() != null) {
                this.aggregations = (InternalAggregations) result.aggregations();
            }
            this.pipelineAggregators = result.pipelineAggregators();
        }
    }

    public TopDocs topDocs() {
        return topDocs;
    }

    /**
     * Returns per match the percolator query id. The key is the Lucene docId of the matching percolator query.
     */
    public Map<Integer, String> ids() {
        return ids;
    }

    public int requestedSize() {
        return requestedSize;
    }

    /**
     * Returns per match the highlight snippets. The key is the Lucene docId of the matching percolator query.
     */
    public Map<Integer, Map<String, HighlightField>> hls() {
        return hls;
    }

    public InternalAggregations aggregations() {
        return aggregations;
    }

    public List<SiblingPipelineAggregator> pipelineAggregators() {
        return pipelineAggregators;
    }

    public boolean onlyCount() {
        return onlyCount;
    }

    public boolean isEmpty() {
        return topDocs.totalHits == 0;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        onlyCount = in.readBoolean();
        requestedSize = in.readVInt();
        topDocs = Lucene.readTopDocs(in);
        int size = in.readVInt();
        ids = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ids.put(in.readVInt(), in.readString());
        }
        size = in.readVInt();
        hls = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            int docId = in.readVInt();
            int mSize = in.readVInt();
            Map<String, HighlightField> fields = new HashMap<>();
            for (int j = 0; j < mSize; j++) {
                fields.put(in.readString(), HighlightField.readHighlightField(in));
            }
            hls.put(docId, fields);
        }
        aggregations = InternalAggregations.readOptionalAggregations(in);
        if (in.readBoolean()) {
            int pipelineAggregatorsSize = in.readVInt();
            List<SiblingPipelineAggregator> pipelineAggregators = new ArrayList<>(pipelineAggregatorsSize);
            for (int i = 0; i < pipelineAggregatorsSize; i++) {
                BytesReference type = in.readBytesReference();
                PipelineAggregator pipelineAggregator = PipelineAggregatorStreams.stream(type).readResult(in);
                pipelineAggregators.add((SiblingPipelineAggregator) pipelineAggregator);
            }
            this.pipelineAggregators = pipelineAggregators;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(onlyCount);
        out.writeVLong(requestedSize);
        Lucene.writeTopDocs(out, topDocs);
        out.writeVInt(ids.size());
        for (Map.Entry<Integer, String> entry : ids.entrySet()) {
            out.writeVInt(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(hls.size());
        for (Map.Entry<Integer, Map<String, HighlightField>> entry1 : hls.entrySet()) {
            out.writeVInt(entry1.getKey());
            out.writeVInt(entry1.getValue().size());
            for (Map.Entry<String, HighlightField> entry2 : entry1.getValue().entrySet()) {
                out.writeString(entry2.getKey());
                entry2.getValue().writeTo(out);
            }
        }
        out.writeOptionalStreamable(aggregations);
        if (pipelineAggregators == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(pipelineAggregators.size());
            for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
                out.writeBytesReference(pipelineAggregator.type().stream());
                pipelineAggregator.writeTo(out);
            }
        }
    }
}
