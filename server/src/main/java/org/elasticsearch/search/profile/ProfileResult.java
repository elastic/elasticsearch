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

package org.elasticsearch.search.profile;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class is the internal representation of a profiled Query, corresponding
 * to a single node in the query tree.  It is built after the query has finished executing
 * and is merely a structured representation, rather than the entity that collects the timing
 * profile (see InternalProfiler for that)
 *
 * Each InternalProfileResult has a List of InternalProfileResults, which will contain
 * "children" queries if applicable
 */
public final class ProfileResult implements Writeable, ToXContentObject {
    static final ParseField TYPE = new ParseField("type");
    static final ParseField DESCRIPTION = new ParseField("description");
    static final ParseField BREAKDOWN = new ParseField("breakdown");
    static final ParseField NODE_TIME = new ParseField("time");
    static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    static final ParseField CHILDREN = new ParseField("children");

    private final String type;
    private final String description;
    private final Map<String, Object> breakdown;
    private final long nodeTime;
    private final List<ProfileResult> children;

    public ProfileResult(String type, String description, Map<String, Object> breakdown, long nodeTime, List<ProfileResult> children) {
        this.type = type;
        this.description = description;
        this.breakdown = Objects.requireNonNull(breakdown, "required breakdown argument missing");
        this.children = children == null ? List.of() : children;
        this.nodeTime = nodeTime;
    }

    /**
     * Read from a stream.
     */
    public ProfileResult(StreamInput in) throws IOException{
        this.type = in.readString();
        this.description = in.readString();
        this.nodeTime = in.readLong();

        if (in.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO backport to 7.9.0
            breakdown = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        } else {
            breakdown = in.readMap(StreamInput::readString, StreamInput::readLong);
        }

        int size = in.readVInt();
        this.children = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            children.add(new ProfileResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(description);
        out.writeLong(nodeTime);            // not Vlong because can be negative
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO backport to 7.9.0
            out.writeMap(breakdown, StreamOutput::writeString, StreamOutput::writeGenericValue);
        } else {
            // Old versions only support numeric breakdowns
            out.writeVInt((int) breakdown.values().stream().filter(o -> o instanceof Number).count());
            for (Map.Entry<String, Object> entry : breakdown.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    out.writeString(entry.getKey());
                    out.writeLong(((Number) entry.getValue()).longValue());
                }
            }
        }
        out.writeVInt(children.size());
        for (ProfileResult child : children) {
            child.writeTo(out);
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
     * Returns the timing breakdown for this node
     */
    public Map<String, Object> getBreakdown() {
        return Collections.unmodifiableMap(breakdown);
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

        if (!children.isEmpty()) {
            builder = builder.startArray(CHILDREN.getPreferredName());
            for (ProfileResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder = builder.endArray();
        }

        builder = builder.endObject();
        return builder;
    }

    private static final InstantiatingObjectParser<ProfileResult, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser =
                InstantiatingObjectParser.builder("profile_result", true, ProfileResult.class);
        parser.declareString(constructorArg(), TYPE);
        parser.declareString(constructorArg(), DESCRIPTION);
        parser.declareObject(constructorArg(), (p, c) -> p.map(), BREAKDOWN);
        parser.declareLong(constructorArg(), NODE_TIME_RAW);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> fromXContent(p), CHILDREN);
        PARSER = parser.build();
    }
    public static ProfileResult fromXContent(XContentParser p) throws IOException {
        return PARSER.parse(p, null);
    }
}
