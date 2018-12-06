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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

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
    static final ParseField NODE_TIME = new ParseField("time");
    static final ParseField NODE_TIME_RAW = new ParseField("time_in_nanos");
    static final ParseField CHILDREN = new ParseField("children");
    static final ParseField BREAKDOWN = new ParseField("breakdown");

    private final String type;
    private final String description;
    private final Map<String, Long> timings;
    private final long nodeTime;
    private final List<ProfileResult> children;

    public ProfileResult(String type, String description, Map<String, Long> timings, List<ProfileResult> children) {
        this.type = type;
        this.description = description;
        this.timings = Objects.requireNonNull(timings, "required timings argument missing");
        this.children = children;
        this.nodeTime = getTotalTime(timings);
    }

    /**
     * Read from a stream.
     */
    public ProfileResult(StreamInput in) throws IOException{
        this.type = in.readString();
        this.description = in.readString();
        this.nodeTime = in.readLong();

        int timingsSize = in.readVInt();
        this.timings = new HashMap<>(timingsSize);
        for (int i = 0; i < timingsSize; ++i) {
            timings.put(in.readString(), in.readLong());
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
        out.writeVInt(timings.size());
        for (Map.Entry<String, Long> entry : timings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
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
     * Retrieve the name of the query (e.g. "TermQuery")
     */
    public String getQueryName() {
        return type;
    }

    /**
     * Returns the timing breakdown for this particular query node
     */
    public Map<String, Long> getTimeBreakdown() {
        return Collections.unmodifiableMap(timings);
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
        builder.field(BREAKDOWN.getPreferredName(), timings);

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

    public static ProfileResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        String currentFieldName = null;
        String type = null, description = null;
        Map<String, Long> timings =  new HashMap<>();
        List<ProfileResult> children = new ArrayList<>();
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = parser.text();
                } else if (DESCRIPTION.match(currentFieldName, parser.getDeprecationHandler())) {
                    description = parser.text();
                } else if (NODE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    // skip, total time is calculate by adding up 'timings' values in ProfileResult ctor
                    parser.text();
                } else if (NODE_TIME_RAW.match(currentFieldName, parser.getDeprecationHandler())) {
                    // skip, total time is calculate by adding up 'timings' values in ProfileResult ctor
                    parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BREAKDOWN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
                        String name = parser.currentName();
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser::getTokenLocation);
                        long value = parser.longValue();
                        timings.put(name, value);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CHILDREN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        children.add(ProfileResult.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new ProfileResult(type, description, timings, children);
    }

    /**
     * @param timings a map of breakdown timing for the node
     * @return The total time at this node
     */
    private static long getTotalTime(Map<String, Long> timings) {
        long nodeTime = 0;
        for (long time : timings.values()) {
            nodeTime += time;
        }
        return nodeTime;
    }

}
