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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class PercolateResponse extends BroadcastOperationResponse implements Iterable<PercolateResponse.Match> {

    private long tookInMillis;
    private Match[] matches;

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, Match[] matches, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = matches;
    }

    public PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
    }

    PercolateResponse() {
    }

    public PercolateResponse(Match[] matches) {
        this.matches = matches;
    }

    /**
     * How long the percolate took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the percolate took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    public Match[] getMatches() {
        return this.matches;
    }

    @Override
    public Iterator<Match> iterator() {
        return Arrays.asList(matches).iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        int size = in.readVInt();
        matches = new Match[size];
        for (int i = 0; i < size; i++) {
            matches[i] = new Match();
            matches[i].readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeVInt(matches.length);
        for (Match match : matches) {
            match.writeTo(out);
        }
    }

    public static class Match implements Streamable {

        private Text id;
        private Text index;

        public Match(Text id, Text index) {
            this.id = id;
            this.index = index;
        }

        Match() {
        }

        public Text id() {
            return id;
        }

        public Text index() {
            return index;
        }

        public Text getId() {
            return id;
        }

        public Text getIndex() {
            return index;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readText();
            index = in.readText();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeText(id);
            out.writeText(index);
        }
    }
}
