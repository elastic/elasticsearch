/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class holds the runtime statistics of a job.  The stats are not used by any internal process
 * and are only for external monitoring/reference.  Statistics are not persisted with the job, so if the
 * allocated task is shutdown/restarted on a different node all the stats will reset.
 */
public class RollupJobStats implements ToXContentObject, Writeable {

    public static final ParseField NAME = new ParseField("job_stats");

    private static ParseField NUM_PAGES = new ParseField("pages_processed");
    private static ParseField NUM_DOCUMENTS = new ParseField("documents_processed");
    private static ParseField NUM_ROLLUPS = new ParseField("rollups_indexed");
    private static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");

    private long numPages = 0;
    private long numDocuments = 0;
    private long numRollups = 0;
    private long numInvocations = 0;

    public static final ConstructingObjectParser<RollupJobStats, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(),
                    args -> new RollupJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

    static {
        PARSER.declareLong(constructorArg(), NUM_PAGES);
        PARSER.declareLong(constructorArg(), NUM_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_ROLLUPS);
        PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
    }

    public RollupJobStats() {
    }

    public RollupJobStats(long numPages, long numDocuments, long numRollups, long numInvocations) {
        this.numPages = numPages;
        this.numDocuments = numDocuments;
        this.numRollups = numRollups;
        this.numInvocations = numInvocations;
    }

    public RollupJobStats(StreamInput in) throws IOException {
        this.numPages = in.readVLong();
        this.numDocuments = in.readVLong();
        this.numRollups = in.readVLong();
        this.numInvocations = in.readVLong();
    }

    public long getNumPages() {
        return numPages;
    }

    public long getNumDocuments() {
        return numDocuments;
    }

    public long getNumInvocations() {
        return numInvocations;
    }

    public long getNumRollups() {
        return numRollups;
    }

    public void incrementNumPages(long n) {
        assert(n >= 0);
        numPages += n;
    }

    public void incrementNumDocuments(long n) {
        assert(n >= 0);
        numDocuments += n;
    }

    public void incrementNumInvocations(long n) {
        assert(n >= 0);
        numInvocations += n;
    }

    public void incrementNumRollups(long n) {
        assert(n >= 0);
        numRollups += n;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numPages);
        out.writeVLong(numDocuments);
        out.writeVLong(numRollups);
        out.writeVLong(numInvocations);
    }

    public static RollupJobStats fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_PAGES.getPreferredName(), numPages);
        builder.field(NUM_DOCUMENTS.getPreferredName(), numDocuments);
        builder.field(NUM_ROLLUPS.getPreferredName(), numRollups);
        builder.field(NUM_INVOCATIONS.getPreferredName(), numInvocations);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollupJobStats that = (RollupJobStats) other;

        return Objects.equals(this.numPages, that.numPages)
                && Objects.equals(this.numDocuments, that.numDocuments)
                && Objects.equals(this.numRollups, that.numRollups)
                && Objects.equals(this.numInvocations, that.numInvocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPages, numDocuments, numRollups, numInvocations);
    }

}

