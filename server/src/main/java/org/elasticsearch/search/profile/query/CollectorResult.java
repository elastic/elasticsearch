/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.sandbox.search.ProfilerCollectorResult;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Public interface and serialization container for profiled timings of the
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 */
public class CollectorResult extends ProfilerCollectorResult implements ToXContentObject, Writeable {

    public static final String REASON_SEARCH_COUNT = "search_count";
    public static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    public static final String REASON_SEARCH_MULTI = "search_multi";
    public static final String REASON_SEARCH_QUERY_PHASE = "search_query_phase";
    public static final String REASON_AGGREGATION = "aggregation";
    public static final String REASON_AGGREGATION_GLOBAL = "aggregation_global";

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField TIME = new ParseField("time");
    private static final ParseField TIME_NANOS = new ParseField("time_in_nanos");
    private static final ParseField CHILDREN = new ParseField("children");

    public CollectorResult(String collectorName, String reason, long time, List<CollectorResult> children) {
        super(collectorName, reason, time, new ArrayList<>(children));
    }

    /**
     * Read from a stream.
     */
    public CollectorResult(StreamInput in) throws IOException {
        super(in.readString(), in.readString(), in.readLong(), in.readList(CollectorResult::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeString(getReason());
        out.writeLong(getTime());
        out.writeList(getCollectorResults());
    }

    public List<CollectorResult> getCollectorResults() {
        return super.getProfiledChildren().stream().map(profilerCollectorResult -> (CollectorResult) profilerCollectorResult).toList();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CollectorResult other = (CollectorResult) obj;
        return getName().equals(other.getName())
            && getReason().equals(other.getReason())
            && getTime() == other.getTime()
            && getCollectorResults().equals(other.getCollectorResults());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getReason(), getTime(), getCollectorResults());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder = builder.startObject();
        builder.field(NAME.getPreferredName(), getName());
        builder.field(REASON.getPreferredName(), getReason());
        if (builder.humanReadable()) {
            builder.field(TIME.getPreferredName(), new TimeValue(getTime(), TimeUnit.NANOSECONDS).toString());
        }
        builder.field(TIME_NANOS.getPreferredName(), getTime());

        if (getProfiledChildren().isEmpty() == false) {
            builder = builder.startArray(CHILDREN.getPreferredName());
            for (CollectorResult child : getCollectorResults()) {
                builder = child.toXContent(builder, params);
            }
            builder = builder.endArray();
        }
        builder = builder.endObject();
        return builder;
    }

    public static CollectorResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        String name = null, reason = null;
        long time = -1;
        List<CollectorResult> children = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    name = parser.text();
                } else if (REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else if (TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    // we need to consume this value, but we use the raw nanosecond value
                    parser.text();
                } else if (TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CHILDREN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        children.add(CollectorResult.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new CollectorResult(name, reason, time, children);
    }
}
