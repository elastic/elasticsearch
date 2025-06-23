/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField REASON = new ParseField("reason");
    public static final ParseField TIME = new ParseField("time");
    public static final ParseField TIME_NANOS = new ParseField("time_in_nanos");
    public static final ParseField CHILDREN = new ParseField("children");

    public CollectorResult(String collectorName, String reason, long time, List<CollectorResult> children) {
        super(collectorName, reason, time, new ArrayList<>(children));
    }

    /**
     * Read from a stream.
     */
    public CollectorResult(StreamInput in) throws IOException {
        super(in.readString(), in.readString(), in.readLong(), in.readCollectionAsList(CollectorResult::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeString(getReason());
        out.writeLong(getTime());
        out.writeCollection(getChildrenResults());
    }

    /**
     * Exposes a list of children collector results. Same as {@link ProfilerCollectorResult#getProfiledChildren()} with each
     * item in the list being cast to a {@link CollectorResult}
     */
    public List<CollectorResult> getChildrenResults() {
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
            && getChildrenResults().equals(other.getChildrenResults());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getReason(), getTime(), getChildrenResults());
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
            for (CollectorResult child : getChildrenResults()) {
                builder = child.toXContent(builder, params);
            }
            builder = builder.endArray();
        }
        builder = builder.endObject();
        return builder;
    }

}
