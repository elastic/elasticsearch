/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The result of a profiled *thing*, like a query or an aggregation. See
 * {@link AbstractProfiler} for the statistic collection framework.
 */
public final class ProfileResult implements Writeable, ToXContentObject {
    static final ParseField TYPE = new ParseField("type");
    static final ParseField DESCRIPTION = new ParseField("description");
    static final ParseField BREAKDOWN = new ParseField("breakdown");
    static final ParseField DEBUG = new ParseField("debug");
    static final ParseField NODE_TIME = new ParseField("time");
    static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    static final ParseField CHILDREN = new ParseField("children");

    private final String type;
    private final String description;
    private final Map<String, Long> breakdown;
    private final Map<String, Object> debug;
    private final long nodeTime;
    private final List<ProfileResult> children;

    public ProfileResult(
        String type,
        String description,
        Map<String, Long> breakdown,
        Map<String, Object> debug,
        long nodeTime,
        List<ProfileResult> children
    ) {
        this.type = type;
        this.description = description;
        this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
        this.debug = debug == null ? Map.of() : debug;
        this.children = children == null ? List.of() : children;
        this.nodeTime = nodeTime;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(description);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        out.writeMap(breakdown, StreamOutput::writeLong);
        out.writeMap(debug, StreamOutput::writeGenericValue);
        out.writeCollection(children);
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
            && children.equals(other.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, description, breakdown, debug, nodeTime, children);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static final InstantiatingObjectParser<ProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "profile_result",
            true,
            ProfileResult.class
        );
        parser.declareString(constructorArg(), TYPE);
        parser.declareString(constructorArg(), DESCRIPTION);
        parser.declareObject(
            constructorArg(),
            (p, c) -> p.map().entrySet().stream().collect(toMap(Map.Entry::getKey, e -> ((Number) e.getValue()).longValue())),
            BREAKDOWN
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), DEBUG);
        parser.declareLong(constructorArg(), NODE_TIME_RAW);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> fromXContent(p), CHILDREN);
        PARSER = parser.build();
    }

    public static ProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
