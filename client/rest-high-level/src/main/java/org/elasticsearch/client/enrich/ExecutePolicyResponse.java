/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public final class ExecutePolicyResponse {

    private static final ParseField TASK_FIELD = new ParseField("task");
    private static final ParseField STATUS_FIELD = new ParseField("status");

    private static final ConstructingObjectParser<ExecutePolicyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "execute_policy_response",
        true,
        args -> new ExecutePolicyResponse((String) args[0], (ExecutionStatus) args[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TASK_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ExecutionStatus.PARSER, STATUS_FIELD);
    }

    public static ExecutePolicyResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String taskId;
    private final ExecutionStatus executionStatus;

    ExecutePolicyResponse(String taskId, ExecutionStatus executionStatus) {
        this.taskId = taskId;
        this.executionStatus = executionStatus;
    }

    public String getTaskId() {
        return taskId;
    }

    public ExecutionStatus getExecutionStatus() {
        return executionStatus;
    }

    public static final class ExecutionStatus {

        private static final ParseField PHASE_FIELD = new ParseField("phase");

        private static final ConstructingObjectParser<ExecutionStatus, Void> PARSER = new ConstructingObjectParser<>(
            "execution_status",
            true,
            args -> new ExecutionStatus((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE_FIELD);
        }

        private final String phase;

        ExecutionStatus(String phase) {
            this.phase = phase;
        }

        public String getPhase() {
            return phase;
        }
    }
}
