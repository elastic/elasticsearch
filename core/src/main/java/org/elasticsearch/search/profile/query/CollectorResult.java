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

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Public interface and serialization container for profiled timings of the
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 */
public class CollectorResult implements ToXContent, Writeable {

    public static final String REASON_SEARCH_COUNT = "search_count";
    public static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    public static final String REASON_SEARCH_TERMINATE_AFTER_COUNT = "search_terminate_after_count";
    public static final String REASON_SEARCH_POST_FILTER = "search_post_filter";
    public static final String REASON_SEARCH_MIN_SCORE = "search_min_score";
    public static final String REASON_SEARCH_MULTI = "search_multi";
    public static final String REASON_SEARCH_TIMEOUT = "search_timeout";
    public static final String REASON_AGGREGATION = "aggregation";
    public static final String REASON_AGGREGATION_GLOBAL = "aggregation_global";

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField TIME = new ParseField("time");
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
        builder = builder.startObject()
                .field(NAME.getPreferredName(), getName())
                .field(REASON.getPreferredName(), getReason())
                .field(TIME.getPreferredName(), String.format(Locale.US, "%.10gms", (double) (getTime() / 1000000.0)));

        if (!children.isEmpty()) {
            builder = builder.startArray(CHILDREN.getPreferredName());
            for (CollectorResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder = builder.endArray();
        }
        builder = builder.endObject();
        return builder;
    }
}
