/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class ConstraintsContainer implements ToXContentObject, Writeable {

    public static class TierLevelConstraints extends NamedValuesContainer {

        public TierLevelConstraints(final Map<String, Integer> valueMap) {
            super(valueMap);
        }

        public TierLevelConstraints(final StreamInput input) throws IOException {
            super(input);
        }

    }

    public static class NodeLevelConstraints extends NamedValuesContainer {

        public NodeLevelConstraints(final Map<String, Integer> valueMap) {
            super(valueMap);
        }

        public NodeLevelConstraints(final StreamInput input) throws IOException {
            super(input);
        }

    }

    private final TierLevelConstraints tierConstraints;
    private final NodeLevelConstraints nodeConstraints;

    public ConstraintsContainer(final StreamInput input) throws IOException {
        this.tierConstraints = new TierLevelConstraints(input);
        this.nodeConstraints = new NodeLevelConstraints(input);
    }

    public ConstraintsContainer(final TierLevelConstraints tierConstraints, final NodeLevelConstraints nodeConstraints) {
        this.tierConstraints = tierConstraints;
        this.nodeConstraints = nodeConstraints;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("tier", tierConstraints);
        builder.field("node", nodeConstraints);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        tierConstraints.writeTo(out);
        nodeConstraints.writeTo(out);
    }
}
