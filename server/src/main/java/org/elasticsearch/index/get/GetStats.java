/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GetStats implements Writeable, ToXContentFragment {

    private long existsCount;
    private long existsTimeInMillis;
    private long missingCount;
    private long missingTimeInMillis;
    private long current;

    public GetStats() {
    }

    public GetStats(StreamInput in) throws IOException {
        existsCount = in.readVLong();
        existsTimeInMillis = in.readVLong();
        missingCount = in.readVLong();
        missingTimeInMillis = in.readVLong();
        current = in.readVLong();
    }

    public GetStats(long existsCount, long existsTimeInMillis, long missingCount, long missingTimeInMillis, long current) {
        this.existsCount = existsCount;
        this.existsTimeInMillis = existsTimeInMillis;
        this.missingCount = missingCount;
        this.missingTimeInMillis = missingTimeInMillis;
        this.current = current;
    }

    public void add(GetStats stats) {
        if (stats == null) {
            return;
        }
        current += stats.current;
        addTotals(stats);
    }

    public void addTotals(GetStats stats) {
        if (stats == null) {
            return;
        }
        existsCount += stats.existsCount;
        existsTimeInMillis += stats.existsTimeInMillis;
        missingCount += stats.missingCount;
        missingTimeInMillis += stats.missingTimeInMillis;
        current += stats.current;
    }

    public long getCount() {
        return existsCount + missingCount;
    }

    public long getTimeInMillis() {
        return existsTimeInMillis + missingTimeInMillis;
    }

    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    public long getExistsCount() {
        return this.existsCount;
    }

    public long getExistsTimeInMillis() {
        return this.existsTimeInMillis;
    }

    public TimeValue getExistsTime() {
        return new TimeValue(existsTimeInMillis);
    }

    public long getMissingCount() {
        return this.missingCount;
    }

    public long getMissingTimeInMillis() {
        return this.missingTimeInMillis;
    }

    public TimeValue getMissingTime() {
        return new TimeValue(missingTimeInMillis);
    }

    public long current() {
        return this.current;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.GET);
        builder.field(Fields.TOTAL, getCount());
        builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, getTime());
        builder.field(Fields.EXISTS_TOTAL, existsCount);
        builder.humanReadableField(Fields.EXISTS_TIME_IN_MILLIS, Fields.EXISTS_TIME, getExistsTime());
        builder.field(Fields.MISSING_TOTAL, missingCount);
        builder.humanReadableField(Fields.MISSING_TIME_IN_MILLIS, Fields.MISSING_TIME, getMissingTime());
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String GET = "get";
        static final String TOTAL = "total";
        static final String TIME = "getTime";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String EXISTS_TOTAL = "exists_total";
        static final String EXISTS_TIME = "exists_time";
        static final String EXISTS_TIME_IN_MILLIS = "exists_time_in_millis";
        static final String MISSING_TOTAL = "missing_total";
        static final String MISSING_TIME = "missing_time";
        static final String MISSING_TIME_IN_MILLIS = "missing_time_in_millis";
        static final String CURRENT = "current";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(existsCount);
        out.writeVLong(existsTimeInMillis);
        out.writeVLong(missingCount);
        out.writeVLong(missingTimeInMillis);
        out.writeVLong(current);
    }
}
