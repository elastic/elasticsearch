/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The result of a profiled *thing*, like a query or an aggregation. See
 * {@link AbstractProfiler} for the statistic collection framework.
 */
public final class ProfileResult implements Writeable, ToXContentObject {
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField BREAKDOWN = new ParseField("breakdown");
    public static final ParseField DEBUG = new ParseField("debug");
    static final ParseField NODE_TIME = new ParseField("time");
    public static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    public static final ParseField CHILDREN = new ParseField("children");
    public static final ParseField TOTALS = new ParseField("totals");
    public static final ParseField SEGMENTS = new ParseField("segments");
    public static final ParseField FIELDS = new ParseField("fields");

    private static final TransportVersion CODEC_READ_PROFILING = TransportVersion.fromName("codec_read_profiling");

    private final String type;
    private final String description;
    private final Map<String, Long> breakdown;
    private final Map<String, Object> debug;
    private final long nodeTime;
    private final List<ProfileResult> children;
    private final Map<String, Object> totals;
    private final Map<String, Object> segments;
    private final Map<String, Object> fields;

    /**
     * The codec read maps are rendered by {@link CodecReads} and are sent only to nodes that support the
     * {@code codec_read_profiling} transport version.
     *
     * @param totals   the codec totals of the node, by codec; empty when no instrumented codec opened a reader
     * @param segments the {@code segments} view, by segment key; empty when no selected segment was read
     * @param fields   the {@code fields} view, by field name; empty when no selected field was read
     */
    public ProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<ProfileResult> children,
        Map<String, Object> totals,
        Map<String, Object> segments,
        Map<String, Object> fields
    ) {
        this.type = type;
        this.description = description;
        this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
        this.debug = debug == null ? Map.of() : debug;
        this.children = children == null ? List.of() : children;
        this.nodeTime = nodeTime;
        this.totals = totals == null ? Map.of() : totals;
        this.segments = segments == null ? Map.of() : segments;
        this.fields = fields == null ? Map.of() : fields;
    }

    /**
     * Read from a stream.
     */
    public ProfileResult(StreamInput in) throws IOException {
        this.type = in.readString();
        this.description = in.readString();
        this.nodeTime = in.readLong();
        breakdown = in.readMap(StreamInput::readLong);
        debug = in.readMap(StreamInput::readGenericValue);
        children = in.readCollectionAsList(ProfileResult::new);
        if (in.getTransportVersion().supports(CODEC_READ_PROFILING)) {
            totals = in.readGenericMap();
            segments = in.readGenericMap();
            fields = in.readGenericMap();
        } else {
            totals = Map.of();
            segments = Map.of();
            fields = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(description);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        out.writeMap(breakdown, StreamOutput::writeLong);
        out.writeMap(debug, StreamOutput::writeGenericValue);
        out.writeCollection(children);
        if (out.getTransportVersion().supports(CODEC_READ_PROFILING)) {
            out.writeGenericMap(totals);
            out.writeGenericMap(segments);
            out.writeGenericMap(fields);
        }
    }

    /**
     * Retrieve the lucene description of this query (e.g. the "explain" text)
     */
    public String getLuceneDescription() {
        return description;
    }

    /**
     * Retrieve the name of the entry (e.g. "TermQuery" or "LongTermsAggregator")
     */
    public String getQueryName() {
        return type;
    }

    /**
     * The timing breakdown for this node.
     */
    public Map<String, Long> getTimeBreakdown() {
        return Collections.unmodifiableMap(breakdown);
    }

    /**
     * The debug information about the profiled execution.
     */
    public Map<String, Object> getDebugInfo() {
        return Collections.unmodifiableMap(debug);
    }

    /** The codec totals of this node, by codec, then component, then metric name. */
    public Map<String, Object> getTotals() {
        return Collections.unmodifiableMap(totals);
    }

    /** The {@code segments} view of the codec reads of this node, by segment key. */
    public Map<String, Object> getSegments() {
        return Collections.unmodifiableMap(segments);
    }

    /** The {@code fields} view of the codec reads of this node, by field name. */
    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Returns the total time (inclusive of children) for this query node.
     *
     * @return  elapsed time in nanoseconds
     */
    public long getTime() {
        return nodeTime;
    }

    /**
     * Returns a list of all profiled children queries
     */
    public List<ProfileResult> getProfiledChildren() {
        return Collections.unmodifiableList(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE.getPreferredName(), type);
        builder.field(DESCRIPTION.getPreferredName(), description);
        if (builder.humanReadable()) {
            builder.field(NODE_TIME.getPreferredName(), new TimeValue(getTime(), TimeUnit.NANOSECONDS).toString());
        }
        builder.field(NODE_TIME_RAW.getPreferredName(), getTime());
        builder.field(BREAKDOWN.getPreferredName(), breakdown);
        if (false == debug.isEmpty()) {
            builder.field(DEBUG.getPreferredName(), debug);
        }
        if (false == totals.isEmpty()) {
            builder.field(TOTALS.getPreferredName(), totals);
        }
        if (false == segments.isEmpty()) {
            builder.field(SEGMENTS.getPreferredName(), segments);
        }
        if (false == fields.isEmpty()) {
            builder.field(FIELDS.getPreferredName(), fields);
        }

        if (false == children.isEmpty()) {
            builder.startArray(CHILDREN.getPreferredName());
            for (ProfileResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ProfileResult other = (ProfileResult) obj;
        return type.equals(other.type)
            && description.equals(other.description)
            && breakdown.equals(other.breakdown)
            && debug.equals(other.debug)
            && nodeTime == other.nodeTime
            && children.equals(other.children)
            && totals.equals(other.totals)
            && segments.equals(other.segments)
            && fields.equals(other.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, description, breakdown, debug, nodeTime, children, totals, segments, fields);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
