/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * Represents the recorded error for an index that Data Stream Lifecycle Service encountered.
 */
public record ErrorEntry(long firstOccurrenceTimestamp, String error, long recordedTimestamp, int retryCount)
    implements
        Writeable,
        ToXContentObject,
        Comparable<ErrorEntry> {

    public ErrorEntry(StreamInput in) throws IOException {
        this(in.readLong(), in.readString(), in.readLong(), in.readInt());
    }

    public static final ParseField FIRST_OCCURRENCE_FIELD = new ParseField("first_occurrence");
    public static final ParseField FIRST_OCCURRENCE_MILLIS_FIELD = new ParseField("first_occurrence_millis");
    public static final ParseField MESSAGE_FIELD = new ParseField("message");
    public static final ParseField LAST_RECORDED_MILLIS_FIELD = new ParseField("last_recorded_millis");
    public static final ParseField LAST_RECORDED_FIELD = new ParseField("last_recorded");
    public static final ParseField RETRY_COUNT_FIELD = new ParseField("retry_count");

    /**
     * Creates a new ErrorEntry with the same first occurent timestamp and error message as the provided existing record, but with a fresh
     * timestamp for the latest occurrence and an incremented retry count.
     */
    public static ErrorEntry incrementRetryCount(ErrorEntry existingRecord, LongSupplier nowSupplier) {
        return new ErrorEntry(
            existingRecord.firstOccurrenceTimestamp(),
            existingRecord.error(),
            nowSupplier.getAsLong(),
            existingRecord.retryCount() + 1
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.humanReadableField(
            FIRST_OCCURRENCE_MILLIS_FIELD.getPreferredName(),
            FIRST_OCCURRENCE_FIELD.getPreferredName(),
            new TimeValue(firstOccurrenceTimestamp)
        );
        builder.field(MESSAGE_FIELD.getPreferredName(), error);
        builder.humanReadableField(
            LAST_RECORDED_MILLIS_FIELD.getPreferredName(),
            LAST_RECORDED_FIELD.getPreferredName(),
            new TimeValue(recordedTimestamp)
        );
        builder.field(RETRY_COUNT_FIELD.getPreferredName(), retryCount);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(firstOccurrenceTimestamp);
        out.writeString(error);
        out.writeLong(recordedTimestamp);
        out.writeInt(retryCount);
    }

    /**
     * Compares two error entries by the number of retries, in reversed order by default.
     */
    @Override
    public int compareTo(ErrorEntry o) {
        return Integer.compare(o.retryCount, retryCount);
    }
}
