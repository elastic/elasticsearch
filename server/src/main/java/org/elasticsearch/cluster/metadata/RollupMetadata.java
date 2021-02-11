/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link RollupGroup}s and their names.
 */
public class RollupMetadata extends AbstractDiffable<RollupMetadata> implements ToXContentObject {
    public static final String TYPE = "rollup";
    private static final ParseField ROLLUP = new ParseField("rollup");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RollupMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false,
        a -> new RollupMetadata((Map<String, RollupGroup>) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, RollupGroup> rollupGroups = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                rollupGroups.put(name, RollupGroup.parse(p));
            }
            return rollupGroups;
        }, ROLLUP);
    }

    /** a map with the name of the original index and the group of rollups as a value */
    private final Map<String, RollupGroup> rollupIndices;

    public RollupMetadata(Map<String, RollupGroup> rollupIndices) {
        this.rollupIndices = rollupIndices;
    }

    public RollupMetadata(StreamInput in) throws IOException {
        this.rollupIndices = in.readMap(StreamInput::readString, RollupGroup::new);
    }

    public Map<String, RollupGroup> rollupGroups() {
        return this.rollupIndices;
    }

    public boolean contains(String index) {
        return this.rollupIndices.containsKey(index);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.rollupIndices, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static RollupMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(ROLLUP.getPreferredName(), rollupIndices)
            .endObject();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.rollupIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RollupMetadata other = (RollupMetadata) obj;
        return Objects.equals(this.rollupIndices, other.rollupIndices);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private final Map<String, RollupGroup> rollupIndices = new HashMap<>();

        public Builder putRollupGroup(String name, RollupGroup group) {
            rollupIndices.put(name,  group);
            return this;
        }

        public RollupMetadata build() {
            return new RollupMetadata(rollupIndices);
        }
    }
}
