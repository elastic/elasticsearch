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

package org.elasticsearch.action.admin.indices.segments;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

public class ShardSegments extends BroadcastShardOperationResponse implements Iterable<Segment> {

    private ShardRouting shardRouting;

    private List<Segment> segments;

    ShardSegments() {
    }

    ShardSegments(ShardRouting shardRouting, List<Segment> segments) {
        super(shardRouting.shardId());
        this.shardRouting = shardRouting;
        this.segments = segments;
    }

    @Override
    public Iterator<Segment> iterator() {
        return segments.iterator();
    }

    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public List<Segment> getSegments() {
        return this.segments;
    }

    public int getNumberOfCommitted() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isCommitted()) {
                count++;
            }
        }
        return count;
    }

    public int getNumberOfSearch() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isSearch()) {
                count++;
            }
        }
        return count;
    }

    public static ShardSegments readShardSegments(StreamInput in) throws IOException {
        ShardSegments shard = new ShardSegments();
        shard.readFrom(in);
        return shard;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        int size = in.readVInt();
        if (size == 0) {
            segments = ImmutableList.of();
        } else {
            segments = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                segments.add(Segment.readSegment(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeVInt(segments.size());
        for (Segment segment : segments) {
            segment.writeTo(out);
        }
    }
}