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

import org.elasticsearch.client.core.IndexerJobStats;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response from rollup's get jobs api.
 */
public class GetRollupJobResponse {
    static final ParseField JOBS = new ParseField("jobs");
    static final ParseField CONFIG = new ParseField("config");
    static final ParseField STATS = new ParseField("stats");
    static final ParseField STATUS = new ParseField("status");
    static final ParseField STATE = new ParseField("job_state");
    static final ParseField CURRENT_POSITION = new ParseField("current_position");
    static final ParseField ROLLUPS_INDEXED = new ParseField("rollups_indexed");
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
        return "{jobs=" + jobs.stream().map(Object::toString).collect(joining("\n")) + "\n}";
    }

    public static class JobWrapper {
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
            return "{job=" + job
                    + ", stats=" + stats
                    + ", status=" + status + "}";
        }
    }

    /**
     * The Rollup specialization of stats for the AsyncTwoPhaseIndexer.
     * Note: instead of `documents_indexed`, this XContent show `rollups_indexed`
     */
    public static class RollupIndexerJobStats extends IndexerJobStats {

        RollupIndexerJobStats(long numPages, long numInputDocuments, long numOuputDocuments, long numInvocations,
                              long indexTime, long indexTotal, long searchTime, long searchTotal, long indexFailures, long searchFailures) {
            super(numPages, numInputDocuments, numOuputDocuments, numInvocations,
                    indexTime, searchTime, indexTotal, searchTotal, indexFailures, searchFailures);
        }

        private static final ConstructingObjectParser<RollupIndexerJobStats, Void> PARSER = new ConstructingObjectParser<>(
                STATS.getPreferredName(),
                true,
                args -> new RollupIndexerJobStats((long) args[0], (long) args[1], (long) args[2], (long) args[3],
                    (long) args[4], (long) args[5], (long) args[6], (long) args[7], (long) args[8], (long) args[9]));
        static {
            PARSER.declareLong(constructorArg(), NUM_PAGES);
            PARSER.declareLong(constructorArg(), NUM_INPUT_DOCUMENTS);
            PARSER.declareLong(constructorArg(), ROLLUPS_INDEXED);
            PARSER.declareLong(constructorArg(), NUM_INVOCATIONS);
            PARSER.declareLong(constructorArg(), INDEX_TIME_IN_MS);
            PARSER.declareLong(constructorArg(), INDEX_TOTAL);
            PARSER.declareLong(constructorArg(), SEARCH_TIME_IN_MS);
            PARSER.declareLong(constructorArg(), SEARCH_TOTAL);
            PARSER.declareLong(constructorArg(), INDEX_FAILURES);
            PARSER.declareLong(constructorArg(), SEARCH_FAILURES);
        }
    }

    /**
     * Status of the rollup job.
     */
    public static class RollupJobStatus {
        private final IndexerState state;
        private final Map<String, Object> currentPosition;

        RollupJobStatus(IndexerState state, Map<String, Object> position) {
            this.state = state;
            this.currentPosition = position;
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

        private static final ConstructingObjectParser<RollupJobStatus, Void> PARSER = new ConstructingObjectParser<>(
                STATUS.getPreferredName(),
                true,
                args -> {
                    IndexerState state = (IndexerState) args[0];
                    @SuppressWarnings("unchecked") // We're careful of the contents
                    Map<String, Object> currentPosition = (Map<String, Object>) args[1];
                    return new RollupJobStatus(state, currentPosition);
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

            // Optional to accommodate old versions of state, not used
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), UPGRADED_DOC_ID);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            RollupJobStatus that = (RollupJobStatus) other;
            return Objects.equals(state, that.state)
                    && Objects.equals(currentPosition, that.currentPosition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, currentPosition);
        }

        @Override
        public final String toString() {
            return "{stats=" + state
                    + ", currentPosition=" + currentPosition + "}";
        }
    }
}
