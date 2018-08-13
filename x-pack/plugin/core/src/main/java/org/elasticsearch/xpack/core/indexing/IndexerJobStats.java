/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexing;

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
public class IndexerJobStats implements ToXContentObject, Writeable {

    public static final ParseField NAME = new ParseField("job_stats");

    private static ParseField NUM_PAGES = new ParseField("pages_processed");
    private static ParseField NUM_INPUT_DOCUMENTS = new ParseField("documents_processed");
    // BWC for RollupJobStats
    private static String ROLLUP_BWC_NUM_OUTPUT_DOCUMENTS = "rollups_indexed";
    private static ParseField NUM_OUTPUT_DOCUMENTS = new ParseField("documents_indexed").withDeprecation(ROLLUP_BWC_NUM_OUTPUT_DOCUMENTS);
    private static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");

    private long numPages = 0;
    private long numInputDocuments = 0;
    private long numOuputDocuments = 0;
    private long numInvocations = 0;

    public static final ConstructingObjectParser<IndexerJobStats, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(),
                    args -> new IndexerJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

    static {
        PARSER.declareLong(constructorArg(), NUM_PAGES);
        PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
        PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
    }

    public IndexerJobStats() {
    }

    public IndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations) {
        this.numPages = numPages;
        this.numInputDocuments = numInputDocuments;
        this.numOuputDocuments = numOuputDocuments;
        this.numInvocations = numInvocations;
    }

    public IndexerJobStats(StreamInput in) throws IOException {
        this.numPages = in.readVLong();
        this.numInputDocuments = in.readVLong();
        this.numOuputDocuments = in.readVLong();
        this.numInvocations = in.readVLong();
    }

    public long getNumPages() {
        return numPages;
    }

    public long getNumDocuments() {
        return numInputDocuments;
    }

    public long getNumInvocations() {
        return numInvocations;
    }

    public long getOutputDocuments() {
        return numOuputDocuments;
    }

    public void incrementNumPages(long n) {
        assert(n >= 0);
        numPages += n;
    }

    public void incrementNumDocuments(long n) {
        assert(n >= 0);
        numInputDocuments += n;
    }

    public void incrementNumInvocations(long n) {
        assert(n >= 0);
        numInvocations += n;
    }

    public void incrementNumOutputDocuments(long n) {
        assert(n >= 0);
        numOuputDocuments += n;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numPages);
        out.writeVLong(numInputDocuments);
        out.writeVLong(numOuputDocuments);
        out.writeVLong(numInvocations);
    }

    public static IndexerJobStats fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toUnwrappedXContent(builder, false);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toUnwrappedXContent(XContentBuilder builder, boolean rollupBWC) throws IOException {
        builder.field(NUM_PAGES.getPreferredName(), numPages);
        builder.field(NUM_INPUT_DOCUMENTS.getPreferredName(), numInputDocuments);
        if (rollupBWC) {
            builder.field(ROLLUP_BWC_NUM_OUTPUT_DOCUMENTS, numOuputDocuments);
        } else {
            builder.field(NUM_OUTPUT_DOCUMENTS.getPreferredName(), numOuputDocuments);
        }
        builder.field(NUM_INVOCATIONS.getPreferredName(), numInvocations);
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

        IndexerJobStats that = (IndexerJobStats) other;

        return Objects.equals(this.numPages, that.numPages)
                && Objects.equals(this.numInputDocuments, that.numInputDocuments)
                && Objects.equals(this.numOuputDocuments, that.numOuputDocuments)
                && Objects.equals(this.numInvocations, that.numInvocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPages, numInputDocuments, numOuputDocuments, numInvocations);
    }
}
