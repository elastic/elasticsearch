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

package org.elasticsearch.client.rollup;

import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static java.util.Collections.unmodifiableList;

/**
 * Response from rollup's get jobs api.
 */
public class GetRollupJobResponse implements ToXContentObject {
    private static final ParseField JOBS = new ParseField("jobs");
    private static final ParseField CONFIG = new ParseField("config");
    private static final ParseField STATS = new ParseField("stats");
    private static final ParseField STATUS = new ParseField("status");
    private static ParseField NUM_PAGES = new ParseField("pages_processed");
    private static ParseField NUM_INPUT_DOCUMENTS = new ParseField("documents_processed");
    private static ParseField NUM_OUTPUT_DOCUMENTS = new ParseField("rollups_indexed");
    private static ParseField NUM_INVOCATIONS = new ParseField("trigger_count");
    private static final ParseField STATE = new ParseField("job_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField UPGRADED_DOC_ID = new ParseField("upgraded_doc_id");

    private List<JobWrapper> jobs;

    GetRollupJobResponse(final List<JobWrapper> jobs) {
        this.jobs = Objects.requireNonNull(jobs, "jobs is required");
    }

    /**
     * Jobs returned by the request.
     */
    public List<JobWrapper> getJobs() {
        return jobs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GetRollupJobResponse that = (GetRollupJobResponse) o;
        return jobs.equals(that.jobs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // XContentBuilder does not support passing the params object for Iterables
        builder.field(JOBS.getPreferredName());
        builder.startArray();
        for (JobWrapper job : jobs) {
            job.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<GetRollupJobResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_rollup_job_response",
            true,
            args -> {
                @SuppressWarnings("unchecked") // We're careful about the type in the list
                List<JobWrapper> jobs = (List<JobWrapper>) args[0];
                return new GetRollupJobResponse(unmodifiableList(jobs));
            });
    static {
        PARSER.declareObjectArray(constructorArg(), JobWrapper.PARSER::apply, JOBS);
    }

    public static GetRollupJobResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    public static class JobWrapper implements ToXContentObject {
        private final RollupJobConfig job;
        private final RollupIndexerJobStats stats;
        private final RollupJobStatus status;

        JobWrapper(RollupJobConfig job, RollupIndexerJobStats stats, RollupJobStatus status) {
            this.job = job;
            this.stats = stats;
            this.status = status;
        }

        /**
         * Configuration of the job.
         */
        public RollupJobConfig getJob() {
            return job;
        }

        /**
         * Statistics about the execution of the job.
         */
        public RollupIndexerJobStats getStats() {
            return stats;
        }

        /**
         * Current state of the job.
         */
        public RollupJobStatus getStatus() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONFIG.getPreferredName());
            job.toXContent(builder, params);
            builder.field(STATUS.getPreferredName(), status);
            builder.field(STATS.getPreferredName(), stats, params);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<JobWrapper, Void> PARSER = new ConstructingObjectParser<>(
                "job",
                true,
                a -> new JobWrapper((RollupJobConfig) a[0], (RollupIndexerJobStats) a[1], (RollupJobStatus) a[2]));
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> RollupJobConfig.fromXContent(p, null), CONFIG);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), RollupIndexerJobStats.PARSER::apply, STATS);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), RollupJobStatus.PARSER::apply, STATUS);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            JobWrapper other = (JobWrapper) obj;
            return Objects.equals(job, other.job)
                    && Objects.equals(stats, other.stats)
                    && Objects.equals(status, other.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(job, stats, status);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    /**
     * The Rollup specialization of stats for the AsyncTwoPhaseIndexer.
     * Note: instead of `documents_indexed`, this XContent show `rollups_indexed`
     */
    public static class RollupIndexerJobStats implements ToXContentObject {
        private final long numPages;
        private final long numInputDocuments;
        private final long numOuputDocuments;
        private final long numInvocations;

        RollupIndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations) {
            this.numPages = numPages;
            this.numInputDocuments = numInputDocuments;
            this.numOuputDocuments = numOuputDocuments;
            this.numInvocations = numInvocations;
        }

        /**
         * The number of pages read from the input indices.
         */
        public long getNumPages() {
            return numPages;
        }

        /**
         * The number of documents read from the input indices.
         */
        public long getNumDocuments() {
            return numInputDocuments;
        }

        /**
         * Number of times that the job woke up to write documents.
         */
        public long getNumInvocations() {
            return numInvocations;
        }

        /**
         * Number of documents written to the result indices.
         */
        public long getOutputDocuments() {
            return numOuputDocuments;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NUM_PAGES.getPreferredName(), numPages);
            builder.field(NUM_INPUT_DOCUMENTS.getPreferredName(), numInputDocuments);
            builder.field(NUM_OUTPUT_DOCUMENTS.getPreferredName(), numOuputDocuments);
            builder.field(NUM_INVOCATIONS.getPreferredName(), numInvocations);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<RollupIndexerJobStats, Void> PARSER = new ConstructingObjectParser<>(
                STATS.getPreferredName(),
                true,
                args -> new RollupIndexerJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3]));
        static {
            PARSER.declareLong(constructorArg(), NUM_PAGES);
            PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
            PARSER.declareLong(constructorArg(), NUM_OUTPUT_DOCUMENTS);
            PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            RollupIndexerJobStats that = (RollupIndexerJobStats) other;
            return Objects.equals(this.numPages, that.numPages)
                    && Objects.equals(this.numInputDocuments, that.numInputDocuments)
                    && Objects.equals(this.numOuputDocuments, that.numOuputDocuments)
                    && Objects.equals(this.numInvocations, that.numInvocations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numPages, numInputDocuments, numOuputDocuments, numInvocations);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    /**
     * Status of the rollup job.
     */
    public static class RollupJobStatus implements ToXContentObject {
        private final IndexerState state;
        private final Map<String, Object> currentPosition;
        private final boolean upgradedDocumentId;

        RollupJobStatus(IndexerState state, Map<String, Object> position, boolean upgradedDocumentId) {
            this.state = state;
            this.currentPosition = position;
            this.upgradedDocumentId = upgradedDocumentId;
        }

        /**
         * The state of the writer.
         */
        public IndexerState getState() {
            return state;
        }
        /**
         * The current position of the writer.
         */
        public Map<String, Object> getCurrentPosition() {
            return currentPosition;
        }
        /**
         * Flag holds the state of the ID scheme, e.g. if it has been upgraded
         * to the concatenation scheme.
         */
        public boolean getUpgradedDocumentId() {
            return upgradedDocumentId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(STATE.getPreferredName(), state.value());
            if (currentPosition != null) {
                builder.field(CURRENT_POSITION.getPreferredName(), currentPosition);
            }
            builder.field(UPGRADED_DOC_ID.getPreferredName(), upgradedDocumentId);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<RollupJobStatus, Void> PARSER = new ConstructingObjectParser<>(
                STATUS.getPreferredName(),
                true,
                args -> {
                    IndexerState state = (IndexerState) args[0];
                    @SuppressWarnings("unchecked") // We're careful of the contents
                    Map<String, Object> currentPosition = (Map<String, Object>) args[1];
                    Boolean upgradedDocumentId = (Boolean) args[2];
                    return new RollupJobStatus(state, currentPosition, upgradedDocumentId == null ? false : upgradedDocumentId);
                });
        static {
            PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), STATE, ObjectParser.ValueType.STRING);
            PARSER.declareField(optionalConstructorArg(), p -> {
                if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                    return p.map();
                }
                if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                    return null;
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, CURRENT_POSITION, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);

            // Optional to accommodate old versions of state
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), UPGRADED_DOC_ID);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            RollupJobStatus that = (RollupJobStatus) other;
            return Objects.equals(state, that.state)
                    && Objects.equals(currentPosition, that.currentPosition)
                    && upgradedDocumentId == that.upgradedDocumentId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, currentPosition, upgradedDocumentId);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    /**
     * IndexerState represents the internal state of the indexer.  It
     * is also persistent when changing from started/stopped in case the allocated
     * task is restarted elsewhere.
     */
    public enum IndexerState {
        /** Indexer is running, but not actively indexing data (e.g. it's idle). */
        STARTED,

        /** Indexer is actively indexing data. */
        INDEXING,

        /**
         * Transition state to where an indexer has acknowledged the stop
         * but is still in process of halting.
         */
        STOPPING,

        /** Indexer is "paused" and ignoring scheduled triggers. */
        STOPPED,

        /**
         * Something (internal or external) has requested the indexer abort
         * and shutdown.
         */
        ABORTING;

        static IndexerState fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
