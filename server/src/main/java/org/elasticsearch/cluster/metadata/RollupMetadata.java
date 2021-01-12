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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link RollupGroup}s and their names.
 */
public class RollupMetadata implements Metadata.Custom {
    public static final String TYPE = "rollup";
    public static final String SOURCE_INDEX_NAME_META_FIELD = "source_index";
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
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new RollupMetadata.RollupMetadataDiff((RollupMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new RollupMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_11_0;
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
        builder.startObject(ROLLUP.getPreferredName());
        for (Map.Entry<String, RollupGroup> rollup : rollupIndices.entrySet()) {
            builder.field(rollup.getKey(), rollup.getValue());
        }
        builder.endObject();
        return builder;
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

    static class RollupMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, RollupGroup>> rollupIndicesDiff;

        RollupMetadataDiff(RollupMetadata before, RollupMetadata after) {
            this.rollupIndicesDiff = DiffableUtils.diff(before.rollupIndices, after.rollupIndices, DiffableUtils.getStringKeySerializer());
        }

        RollupMetadataDiff(StreamInput in) throws IOException {
            this.rollupIndicesDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                RollupGroup::new, RollupGroup::readDiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new RollupMetadata(rollupIndicesDiff.apply(((RollupMetadata) part).rollupIndices));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            rollupIndicesDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
