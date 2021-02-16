/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Object representing a group of rollup-v2 indices that have been computed on their respective original-indexes.
 * Used by {@link RollupMetadata}. The rollup group is based on a map with the rollup-index name as a key and
 * its rollup information object as value.
 *
 */
// TODO(csoulios): Removed this class because it is no longer used.
public class RollupGroup extends AbstractDiffable<RollupGroup> implements ToXContentObject {
    private static final ParseField GROUP_FIELD = new ParseField("group");

    /** a map from rollup-index name to its rollup configuration */
    private final Map<String, RollupIndexMetadata> group;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupGroup, Void> PARSER =
        new ConstructingObjectParser<>("rollup_group", false,
            a -> new RollupGroup((Map<String, RollupIndexMetadata>) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, RollupIndexMetadata> rollupGroups = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                rollupGroups.put(name, RollupIndexMetadata.parse(p));
            }
            return rollupGroups;
        }, GROUP_FIELD);
    }

    public RollupGroup(Map<String, RollupIndexMetadata> group) {
        this.group = group;
    }

    public RollupGroup() {
        this.group = new HashMap<>();
    }

    public RollupGroup(StreamInput in) throws IOException {
        this.group = in.readMap(StreamInput::readString, RollupIndexMetadata::new);
    }

    public static RollupGroup fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public void add(String name, RollupIndexMetadata rollupIndexMetadata) {
        group.put(name, rollupIndexMetadata);
    }

    public void remove(String name) {
        group.remove(name);
    }

    public boolean contains(String name) {
        return group.containsKey(name);
    }

    public DateHistogramInterval getDateInterval(String name) {
        RollupIndexMetadata rollupIndex = group.get(name);
        return rollupIndex != null ? rollupIndex.getDateInterval() : null;
    }

    public WriteableZoneId getDateTimezone(String name) {
        RollupIndexMetadata rollupIndex = group.get(name);
        return rollupIndex != null ? rollupIndex.getDateTimezone() : null;
    }

    public Set<String> getIndices() {
        return group.keySet();
    }

    static Diff<RollupGroup> readDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(RollupGroup::new, in);
    }

    public static RollupGroup parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(group, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(GROUP_FIELD.getPreferredName(), group)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupGroup that = (RollupGroup) o;
        return Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group);
    }
}
