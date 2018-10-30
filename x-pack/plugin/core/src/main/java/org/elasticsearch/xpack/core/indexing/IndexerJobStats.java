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
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Objects;

/**
 * This class holds the runtime statistics of a job.  The stats are not used by any internal process
 * and are only for external monitoring/reference.  Statistics are not persisted with the job, so if the
 * allocated task is shutdown/restarted on a different node all the stats will reset.
 */
public abstract class IndexerJobStats implements ToXContentObject, Writeable {

    public static final ParseField NAME = new ParseField("job_stats");

    protected long numPages = 0;
    protected long numInputDocuments = 0;
    protected long numOuputDocuments = 0;
    protected long numInvocations = 0;

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
