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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.percolator.PercolatorService;

import java.io.IOException;

/**
 */
public class PercolateShardResponse extends BroadcastShardOperationResponse {

    private long count;
    private float[] scores;
    private Text[] matches;

    // Request fields:
    private boolean limit;
    private int requestedSize;
    private boolean sort;
    private boolean score;

    public PercolateShardResponse() {
    }

    public PercolateShardResponse(Text[] matches, long count, float[] scores, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.count = count;
        this.scores = scores;
        this.limit = context.limit;
        this.requestedSize = context.size;
        this.sort = context.sort;
        this.score = context.score;
    }

    public PercolateShardResponse(Text[] matches, long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = matches;
        this.scores = new float[0];
        this.count = count;
        this.limit = context.limit;
        this.requestedSize = context.size;
        this.sort = context.sort;
        this.score = context.score;
    }

    public PercolateShardResponse(long count, PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.count = count;
        this.matches = StringText.EMPTY_ARRAY;
        this.scores = new float[0];
        this.limit = context.limit;
        this.requestedSize = context.size;
        this.sort = context.sort;
        this.score = context.score;
    }

    public PercolateShardResponse(PercolatorService.PercolateContext context, String index, int shardId) {
        super(index, shardId);
        this.matches = StringText.EMPTY_ARRAY;
        this.scores = new float[0];
        this.limit = context.limit;
        this.requestedSize = context.size;
        this.sort = context.sort;
        this.score = context.score;
    }

    public Text[] matches() {
        return matches;
    }

    public float[] scores() {
        return scores;
    }

    public long count() {
        return count;
    }

    public boolean limit() {
        return limit;
    }

    public int requestedSize() {
        return requestedSize;
    }

    public boolean sort() {
        return sort;
    }

    public boolean score() {
        return score;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        count = in.readVLong();
        matches = in.readTextArray();
        scores = new float[in.readVInt()];
        for (int i = 0; i < scores.length; i++) {
            scores[i] = in.readFloat();
        }

        limit = in.readBoolean();
        requestedSize = in.readVInt();
        sort = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        out.writeTextArray(matches);
        out.writeVLong(scores.length);
        for (float score : scores) {
            out.writeFloat(score);
        }

        out.writeBoolean(limit);
        out.writeVLong(requestedSize);
        out.writeBoolean(sort);
    }
}
