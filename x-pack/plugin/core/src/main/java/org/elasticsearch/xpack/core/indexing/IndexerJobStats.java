/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
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
    protected long indexTime = 0;
    protected long searchTime = 0;
    protected long indexTotal = 0;
    protected long searchTotal = 0;
    protected long processingTime = 0;
    protected long processingTotal = 0;
    protected long indexFailures = 0;
    protected long searchFailures = 0;

    private long startIndexTime;
    private long startSearchTime;
    private long startProcessingTime;

    public IndexerJobStats() {
    }

    public IndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations,
                           long indexTime, long searchTime, long processingTime, long indexTotal, long searchTotal,
                           long processingTotal, long indexFailures, long searchFailures) {
        this.numPages = numPages;
        this.numInputDocuments = numInputDocuments;
        this.numOuputDocuments = numOuputDocuments;
        this.numInvocations = numInvocations;
        this.indexTime = indexTime;
        this.searchTime = searchTime;
        this.processingTime = processingTime;
        this.indexTotal = indexTotal;
        this.searchTotal = searchTotal;
        this.processingTotal = processingTotal;
        this.indexFailures = indexFailures;
        this.searchFailures = searchFailures;
    }

    public IndexerJobStats(StreamInput in) throws IOException {
        this.numPages = in.readVLong();
        this.numInputDocuments = in.readVLong();
        this.numOuputDocuments = in.readVLong();
        this.numInvocations = in.readVLong();
        this.indexTime = in.readVLong();
        this.searchTime = in.readVLong();
        this.indexTotal = in.readVLong();
        this.searchTotal = in.readVLong();
        this.indexFailures = in.readVLong();
        this.searchFailures = in.readVLong();

        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.processingTime = in.readVLong();
            this.processingTotal = in.readVLong();
        }
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

    public long getIndexFailures() {
        return indexFailures;
    }

    public long getSearchFailures() {
        return searchFailures;
    }

    public long getIndexTime() {
        return indexTime;
    }

    public long getSearchTime() {
        return searchTime;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public long getIndexTotal() {
        return indexTotal;
    }

    public long getSearchTotal() {
        return searchTotal;
    }

    public long getProcessingTotal() {
        return processingTotal;
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

    public void incrementIndexingFailures() {
        this.indexFailures += 1;
    }

    public void incrementSearchFailures() {
        this.searchFailures += 1;
    }

    public void markStartIndexing() {
        this.startIndexTime = System.nanoTime();
    }

    public void markEndIndexing() {
        indexTime += ((System.nanoTime() - startIndexTime) / 1000000);
        indexTotal += 1;
    }

    public void markStartSearch() {
        this.startSearchTime = System.nanoTime();
    }

    public void markEndSearch() {
        searchTime += ((System.nanoTime() - startSearchTime) / 1000000);
        searchTotal += 1;
    }

    public void markStartProcessing() {
        this.startProcessingTime = System.nanoTime();
    }

    public void markEndProcessing() {
        processingTime += ((System.nanoTime() - startProcessingTime) / 1000000);
        processingTotal += 1;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numPages);
        out.writeVLong(numInputDocuments);
        out.writeVLong(numOuputDocuments);
        out.writeVLong(numInvocations);
        out.writeVLong(indexTime);
        out.writeVLong(searchTime);
        out.writeVLong(indexTotal);
        out.writeVLong(searchTotal);
        out.writeVLong(indexFailures);
        out.writeVLong(searchFailures);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeVLong(processingTime);
            out.writeVLong(processingTotal);
        }
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
            && Objects.equals(this.numInvocations, that.numInvocations)
            && Objects.equals(this.indexTime, that.indexTime)
            && Objects.equals(this.searchTime, that.searchTime)
            && Objects.equals(this.processingTime, that.processingTime)
            && Objects.equals(this.indexFailures, that.indexFailures)
            && Objects.equals(this.searchFailures, that.searchFailures)
            && Objects.equals(this.indexTotal, that.indexTotal)
            && Objects.equals(this.searchTotal, that.searchTotal)
            && Objects.equals(this.processingTotal, that.processingTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numPages, numInputDocuments, numOuputDocuments, numInvocations,
            indexTime, searchTime, processingTime, indexFailures, searchFailures, indexTotal, searchTotal, processingTotal);
    }
}
