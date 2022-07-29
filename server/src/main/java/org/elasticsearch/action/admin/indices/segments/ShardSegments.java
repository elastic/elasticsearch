/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.engine.Segment;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ShardSegments implements Writeable, Iterable<Segment> {

    private final ShardRouting shardRouting;

    private final List<Segment> segments;

    ShardSegments(ShardRouting shardRouting, List<Segment> segments) {
        this.shardRouting = shardRouting;
        this.segments = segments;
    }

    ShardSegments(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        segments = in.readList(Segment::new);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeList(segments);
    }
}
