/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class SeekStats extends BaseNodeResponse implements ToXContentFragment {

    private final Map<String, Long> seeks;

    public SeekStats(DiscoveryNode node, Map<String, Long> seeks) {
        super(node);
        this.seeks = seeks;
    }

    public SeekStats(StreamInput in) throws IOException {
        super(in);
        this.seeks = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(seeks, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.mapContents(seeks);
        return builder;
    }

    public boolean isEmpty() {
        return seeks.isEmpty();
    }
}
