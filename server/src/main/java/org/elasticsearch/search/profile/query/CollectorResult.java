/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Public interface and serialization container for profiled timings of the
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 */
public class CollectorResult implements ToXContentObject, Writeable {

    public static final String REASON_SEARCH_COUNT = "search_count";
    public static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    public static final String REASON_SEARCH_TERMINATE_AFTER_COUNT = "search_terminate_after_count";
    public static final String REASON_SEARCH_POST_FILTER = "search_post_filter";
    public static final String REASON_SEARCH_MIN_SCORE = "search_min_score";
    public static final String REASON_SEARCH_MULTI = "search_multi";
    public static final String REASON_AGGREGATION = "aggregation";
    public static final String REASON_AGGREGATION_GLOBAL = "aggregation_global";

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField TIME = new ParseField("time");
    private static final ParseField TIME_NANOS = new ParseField("time_in_nanos");
    private static final ParseField CHILDREN = new ParseField("children");

    /**
     * A more friendly representation of the Collector's class name
     */
    private final String collectorName;

    /**
     * A "hint" to help provide some context about this Collector
     */
    private final String reason;

    /**
     * The total elapsed time for this Collector
     */
    private final Long time;

    /**
     * A list of children collectors "embedded" inside this collector
     */
    private List<CollectorResult> children;

    public CollectorResult(String collectorName, String reason, Long time, List<CollectorResult> children) {
        this.collectorName = collectorName;
        this.reason = reason;
        this.time = time;
        this.children = children;
    }

    /**
     * Read from a stream.
     */
    public CollectorResult(StreamInput in) throws IOException {
        this.collectorName = in.readString();
        this.reason = in.readString();
        this.time = in.readLong();
        int size = in.readVInt();
        this.children = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            CollectorResult child = new CollectorResult(in);
            this.children.add(child);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(collectorName);
        out.writeString(reason);
        out.writeLong(time);
        out.writeVInt(children.size());
        for (CollectorResult child : children) {
            child.writeTo(out);
        }
    }

    /**
     * @return the profiled time for this collector (inclusive of children)
     */
    public long getTime() {
        return this.time;
    }

    /**
     * @return a human readable "hint" about what this collector was used for
     */
    public String getReason() {
        return this.reason;
    }

    /**
     * @return the lucene class name of the collector
     */
    public String getName() {
        return this.collectorName;
    }

    /**
     * @return a list of children collectors
     */
    public List<CollectorResult> getProfiledChildren() {
        return children;
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

        if (children.isEmpty() == false) {
            builder = builder.startArray(CHILDREN.getPreferredName());
            for (CollectorResult child : children) {
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
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
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
