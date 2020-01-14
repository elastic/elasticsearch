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
package org.elasticsearch.client.enrich;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskInfo;

import java.util.List;
import java.util.Objects;

public final class StatsResponse {

    private static ParseField EXECUTING_POLICIES_FIELD = new ParseField("executing_policies");
    private static ParseField EXECUTION_STATS_FIELD = new ParseField("execution_stats");
    private static ParseField COORDINATOR_STATS_FIELD = new ParseField("coordinator_stats");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<StatsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "stats_response",
        true,
        args -> new StatsResponse((List<ExecutingPolicy>) args[0], (ExecutionStats) args[1], (List<CoordinatorStats>) args[2])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), ExecutingPolicy.PARSER::apply, EXECUTING_POLICIES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ExecutionStats.PARSER::apply, EXECUTION_STATS_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), CoordinatorStats.PARSER::apply, COORDINATOR_STATS_FIELD);
    }

    public static StatsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<ExecutingPolicy> executingPolicies;
    private final ExecutionStats executionStats;
    private final List<CoordinatorStats> coordinatorStats;

    public StatsResponse(List<ExecutingPolicy> executingPolicies, ExecutionStats executionStats, List<CoordinatorStats> coordinatorStats) {
        this.executingPolicies = executingPolicies;
        this.executionStats = executionStats;
        this.coordinatorStats = coordinatorStats;
    }

    public List<ExecutingPolicy> getExecutingPolicies() {
        return executingPolicies;
    }

    public ExecutionStats getExecutionStats() {
        return executionStats;
    }

    public List<CoordinatorStats> getCoordinatorStats() {
        return coordinatorStats;
    }

    public static final class CoordinatorStats {

        static ParseField NODE_ID_FIELD = new ParseField("node_id");
        static ParseField QUEUE_SIZE_FIELD = new ParseField("queue_size");
        static ParseField REMOTE_REQUESTS_CONCURRENT_FIELD = new ParseField("remote_requests_current");
        static ParseField REMOTE_REQUESTS_TOTAL_FIELD = new ParseField("remote_requests_total");
        static ParseField EXECUTED_SEARCHES_FIELD = new ParseField("executed_searches_total");

        private static final ConstructingObjectParser<CoordinatorStats, Void> PARSER = new ConstructingObjectParser<>(
            "coordinator_stats_item",
            true,
            args -> new CoordinatorStats((String) args[0], (int) args[1], (int) args[2], (long) args[3], (long) args[4])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), QUEUE_SIZE_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), REMOTE_REQUESTS_CONCURRENT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), REMOTE_REQUESTS_TOTAL_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXECUTED_SEARCHES_FIELD);
        }

        private final String nodeId;
        private final int queueSize;
        private final int remoteRequestsCurrent;
        private final long remoteRequestsTotal;
        private final long executedSearchesTotal;

        public CoordinatorStats(String nodeId,
                                int queueSize,
                                int remoteRequestsCurrent,
                                long remoteRequestsTotal,
                                long executedSearchesTotal) {
            this.nodeId = nodeId;
            this.queueSize = queueSize;
            this.remoteRequestsCurrent = remoteRequestsCurrent;
            this.remoteRequestsTotal = remoteRequestsTotal;
            this.executedSearchesTotal = executedSearchesTotal;
        }

        public String getNodeId() {
            return nodeId;
        }

        public int getQueueSize() {
            return queueSize;
        }

        public int getRemoteRequestsCurrent() {
            return remoteRequestsCurrent;
        }

        public long getRemoteRequestsTotal() {
            return remoteRequestsTotal;
        }

        public long getExecutedSearchesTotal() {
            return executedSearchesTotal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorStats stats = (CoordinatorStats) o;
            return Objects.equals(nodeId, stats.nodeId) &&
                queueSize == stats.queueSize &&
                remoteRequestsCurrent == stats.remoteRequestsCurrent &&
                remoteRequestsTotal == stats.remoteRequestsTotal &&
                executedSearchesTotal == stats.executedSearchesTotal;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, queueSize, remoteRequestsCurrent, remoteRequestsTotal, executedSearchesTotal);
        }
    }

    public static class ExecutionStats {

        static ParseField TOTAL_EXECUTIONS_FIELD = new ParseField("total_executions");
        static ParseField TOTAL_EXECUTION_TIME_FIELD = new ParseField("total_execution_time");
        static ParseField MIN_EXECUTION_TIME_FIELD = new ParseField("min_execution_time");
        static ParseField MAX_EXECUTION_TIME_FIELD = new ParseField("max_execution_time");
        static ParseField TOTAL_REPEAT_EXECUTIONS_FIELD = new ParseField("total_repeat_executions");
        static ParseField TOTAL_TIME_BETWEEN_EXECUTIONS_FIELD = new ParseField("total_time_between_executions");
        static ParseField AVG_TIME_BETWEEN_EXECUTIONS_FIELD = new ParseField("avg_time_between_executions");
        static ParseField MIN_TIME_BETWEEN_EXECUTIONS_FIELD = new ParseField("min_time_between_executions");
        static ParseField MAX_TIME_BETWEEN_EXECUTIONS_FIELD = new ParseField("max_time_between_executions");

        private static final ConstructingObjectParser<ExecutionStats, Void> PARSER = new ConstructingObjectParser<>(
            "execution_stats_item",
            true,
            args -> new ExecutionStats((long) args[0], (long) args[1], (long) args[2], (long) args[3], (long) args[4], (long) args[5],
                (long) args[6], (long) args[7], (long) args[8])
        );

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_EXECUTIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_EXECUTION_TIME_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_EXECUTION_TIME_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_EXECUTION_TIME_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_REPEAT_EXECUTIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_TIME_BETWEEN_EXECUTIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), AVG_TIME_BETWEEN_EXECUTIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MIN_TIME_BETWEEN_EXECUTIONS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MAX_TIME_BETWEEN_EXECUTIONS_FIELD);
        }

        private final Long totalExecutionCount;
        private final Long totalExecutionTime;
        private final Long minExecutionTime;
        private final Long maxExecutionTime;
        private final Long totalRepeatExecutionCount;
        private final Long totalTimeBetweenExecutions;
        private final Long avgTimeBetweenExecutions;
        private final Long minTimeBetweenExecutions;
        private final Long maxTimeBetweenExecutions;

        public ExecutionStats(Long totalExecutionCount, Long totalExecutionTime, Long minExecutionTime, Long maxExecutionTime,
                              Long totalRepeatExecutionCount, Long totalTimeBetweenExecutions, Long avgTimeBetweenExecutions,
                              Long minTimeBetweenExecutions, Long maxTimeBetweenExecutions) {
            this.totalExecutionCount = totalExecutionCount;
            this.totalExecutionTime = totalExecutionTime;
            this.minExecutionTime = minExecutionTime;
            this.maxExecutionTime = maxExecutionTime;
            this.totalRepeatExecutionCount = totalRepeatExecutionCount;
            this.totalTimeBetweenExecutions = totalTimeBetweenExecutions;
            this.avgTimeBetweenExecutions = avgTimeBetweenExecutions;
            this.minTimeBetweenExecutions = minTimeBetweenExecutions;
            this.maxTimeBetweenExecutions = maxTimeBetweenExecutions;
        }

        public Long getTotalExecutionCount() {
            return totalExecutionCount;
        }

        public Long getTotalExecutionTime() {
            return totalExecutionTime;
        }

        public Long getMinExecutionTime() {
            return minExecutionTime;
        }

        public Long getMaxExecutionTime() {
            return maxExecutionTime;
        }

        public Long getTotalRepeatExecutionCount() {
            return totalRepeatExecutionCount;
        }

        public Long getTotalTimeBetweenExecutions() {
            return totalTimeBetweenExecutions;
        }

        public Long getAvgTimeBetweenExecutions() {
            return avgTimeBetweenExecutions;
        }

        public Long getMinTimeBetweenExecutions() {
            return minTimeBetweenExecutions;
        }

        public Long getMaxTimeBetweenExecutions() {
            return maxTimeBetweenExecutions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExecutionStats that = (ExecutionStats) o;
            return totalExecutionCount.equals(that.totalExecutionCount) &&
                totalExecutionTime.equals(that.totalExecutionTime) &&
                minExecutionTime.equals(that.minExecutionTime) &&
                maxExecutionTime.equals(that.maxExecutionTime) &&
                totalRepeatExecutionCount.equals(that.totalRepeatExecutionCount) &&
                totalTimeBetweenExecutions.equals(that.totalTimeBetweenExecutions) &&
                avgTimeBetweenExecutions.equals(that.avgTimeBetweenExecutions) &&
                minTimeBetweenExecutions.equals(that.minTimeBetweenExecutions) &&
                maxTimeBetweenExecutions.equals(that.maxTimeBetweenExecutions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalExecutionCount, totalExecutionTime, minExecutionTime, maxExecutionTime, totalRepeatExecutionCount,
                totalTimeBetweenExecutions, avgTimeBetweenExecutions, minTimeBetweenExecutions, maxTimeBetweenExecutions);
        }
    }

    public static class ExecutingPolicy {

        static ParseField NAME_FIELD = new ParseField("name");
        static ParseField TASK_FIELD = new ParseField("task");

        private static final ConstructingObjectParser<ExecutingPolicy, Void> PARSER = new ConstructingObjectParser<>(
            "executing_policy_item",
            true,
            args -> new ExecutingPolicy((String) args[0], (TaskInfo) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> TaskInfo.fromXContent(p), TASK_FIELD);
        }

        private final String name;
        private final TaskInfo taskInfo;

        public ExecutingPolicy(String name, TaskInfo taskInfo) {
            this.name = name;
            this.taskInfo = taskInfo;
        }

        public String getName() {
            return name;
        }

        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExecutingPolicy that = (ExecutingPolicy) o;
            return name.equals(that.name) &&
                taskInfo.equals(that.taskInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, taskInfo);
        }
    }

}
