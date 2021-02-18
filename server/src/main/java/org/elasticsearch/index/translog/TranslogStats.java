/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.translog;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class TranslogStats implements Writeable, ToXContentFragment {

    private long translogSizeInBytes;
    private int numberOfOperations;
    private long uncommittedSizeInBytes;
    private int  uncommittedOperations;
    private long earliestLastModifiedAge;

    public TranslogStats() {
    }

    public TranslogStats(StreamInput in) throws IOException {
        numberOfOperations = in.readVInt();
        translogSizeInBytes = in.readVLong();
        uncommittedOperations = in.readVInt();
        uncommittedSizeInBytes = in.readVLong();
        earliestLastModifiedAge = in.readVLong();
    }

    public TranslogStats(int numberOfOperations, long translogSizeInBytes, int uncommittedOperations, long uncommittedSizeInBytes,
                         long earliestLastModifiedAge) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        if (uncommittedOperations < 0) {
            throw new IllegalArgumentException("uncommittedOperations must be >= 0");
        }
        if (uncommittedSizeInBytes < 0) {
            throw new IllegalArgumentException("uncommittedSizeInBytes must be >= 0");
        }
        if (earliestLastModifiedAge < 0) {
            throw new IllegalArgumentException("earliestLastModifiedAge must be >= 0");
        }
        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
        this.uncommittedSizeInBytes = uncommittedSizeInBytes;
        this.uncommittedOperations = uncommittedOperations;
        this.earliestLastModifiedAge = earliestLastModifiedAge;
    }

    public void add(TranslogStats translogStats) {
        if (translogStats == null) {
            return;
        }

        this.numberOfOperations += translogStats.numberOfOperations;
        this.translogSizeInBytes += translogStats.translogSizeInBytes;
        this.uncommittedOperations += translogStats.uncommittedOperations;
        this.uncommittedSizeInBytes += translogStats.uncommittedSizeInBytes;
        if (this.earliestLastModifiedAge == 0) {
            this.earliestLastModifiedAge = translogStats.earliestLastModifiedAge;
        } else {
            this.earliestLastModifiedAge =
                Math.min(this.earliestLastModifiedAge, translogStats.earliestLastModifiedAge);
        }
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public int estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    /** the size of the generations in the translog that weren't yet to committed to lucene */
    public long getUncommittedSizeInBytes() {
        return uncommittedSizeInBytes;
    }

    /** the number of operations in generations of the translog that weren't yet to committed to lucene */
    public int getUncommittedOperations() {
        return uncommittedOperations;
    }

    public long getEarliestLastModifiedAge() { return earliestLastModifiedAge; }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("translog");
        builder.field("operations", numberOfOperations);
        builder.humanReadableField("size_in_bytes", "size", new ByteSizeValue(translogSizeInBytes));
        builder.field("uncommitted_operations", uncommittedOperations);
        builder.humanReadableField("uncommitted_size_in_bytes", "uncommitted_size", new ByteSizeValue(uncommittedSizeInBytes));
        builder.field("earliest_last_modified_age", earliestLastModifiedAge);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfOperations);
        out.writeVLong(translogSizeInBytes);
        out.writeVInt(uncommittedOperations);
        out.writeVLong(uncommittedSizeInBytes);
        out.writeVLong(earliestLastModifiedAge);
    }
}
