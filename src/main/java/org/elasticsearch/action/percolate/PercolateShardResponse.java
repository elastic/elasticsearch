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

package org.elasticsearch.action.percolate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.percolator.PercolatorService;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private static final BytesRef[] EMPTY = new BytesRef[0];

    private long count;
    private float[] scores;
    private BytesRef[] matches;
    private byte percolatorTypeId;
    private int requestedSize;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(BytesRef[] matches, long count, float[] scores, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.count = count;
        this.scores = scores;
        this.percolatorTypeId = context.percolatorTypeId;
        this.requestedSize = context.size;
    }

    public PercolateShardResponse(BytesRef[] matches, long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.scores = new float[0];
        this.count = count;
        this.percolatorTypeId = context.percolatorTypeId;
        this.requestedSize = context.size;
    }

    public PercolateShardResponse(long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.count = count;
        this.matches = EMPTY;
        this.scores = new float[0];
        this.percolatorTypeId = context.percolatorTypeId;
        this.requestedSize = context.size;
    }

    public PercolateShardResponse(PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = EMPTY;
        this.scores = new float[0];
        this.requestedSize = context.size;
    }

    public BytesRef[] matches() {
        return matches;
    }

    public float[] scores() {
        return scores;
    }

    public long count() {
        return count;
    }

    public int requestedSize() {
        return requestedSize;
    }

    public byte percolatorTypeId() {
        return percolatorTypeId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readVLong();
        matches = new BytesRef[in.readVInt()];
        for (int i = 0; i < matches.length; i++) {
            matches[i] = in.readBytesRef();
        }
        scores = new float[in.readVInt()];
        for (int i = 0; i < scores.length; i++) {
            scores[i] = in.readFloat();
        }
        percolatorTypeId = in.readByte();
        requestedSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        out.writeVInt(matches.length);
        for (BytesRef match : matches) {
            out.writeBytesRef(match);
        }
        out.writeVLong(scores.length);
        for (float score : scores) {
            out.writeFloat(score);
        }
        out.writeByte(percolatorTypeId);
        out.writeVLong(requestedSize);
    }
}
