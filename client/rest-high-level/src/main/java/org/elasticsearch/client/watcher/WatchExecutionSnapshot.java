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

package org.elasticsearch.client.watcher;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class WatchExecutionSnapshot {
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<WatchExecutionSnapshot, Void> PARSER =
        new ConstructingObjectParser<>("watcher_stats_node", true, (args, c) -> new WatchExecutionSnapshot(
            (String) args[0],
            (String) args[1],
            DateTime.parse((String)  args[2]),
            DateTime.parse((String)  args[3]),
            ExecutionPhase.valueOf(((String) args[4]).toUpperCase(Locale.ROOT)),
            args[5] == null ? null : ((List<String>) args[5]).toArray(new String[0]),
            args[6] == null ? null : ((List<String>) args[6]).toArray(new String[0])
        ));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("watch_id"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("watch_record_id"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("triggered_time"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("execution_time"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("execution_phase"));
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField("executed_actions"));
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField("stack_trace"));
    }

    private final String watchId;
    private final String watchRecordId;
    private final DateTime triggeredTime;
    private final DateTime executionTime;
    private final ExecutionPhase phase;
    private final String[] executedActions;
    private final String[] executionStackTrace;

    public WatchExecutionSnapshot(String watchId, String watchRecordId, DateTime triggeredTime, DateTime executionTime,
                                  ExecutionPhase phase, String[] executedActions, String[] executionStackTrace) {
        this.watchId = watchId;
        this.watchRecordId = watchRecordId;
        this.triggeredTime = triggeredTime;
        this.executionTime = executionTime;
        this.phase = phase;
        this.executedActions = executedActions;
        this.executionStackTrace = executionStackTrace;
    }

    public String getWatchId() {
        return watchId;
    }

    public String getWatchRecordId() {
        return watchRecordId;
    }

    public DateTime getTriggeredTime() {
        return triggeredTime;
    }

    public DateTime getExecutionTime() {
        return executionTime;
    }

    public ExecutionPhase getPhase() {
        return phase;
    }

    public String[] getExecutedActions() {
        return executedActions;
    }

    public String[] getExecutionStackTrace() {
        return executionStackTrace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WatchExecutionSnapshot that = (WatchExecutionSnapshot) o;
        return Objects.equals(watchId, that.watchId) &&
            Objects.equals(watchRecordId, that.watchRecordId) &&
            Objects.equals(triggeredTime, that.triggeredTime) &&
            Objects.equals(executionTime, that.executionTime) &&
            phase == that.phase &&
            Arrays.equals(executedActions, that.executedActions) &&
            Arrays.equals(executionStackTrace, that.executionStackTrace);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(watchId, watchRecordId, triggeredTime, executionTime, phase);
        result = 31 * result + Arrays.hashCode(executedActions);
        result = 31 * result + Arrays.hashCode(executionStackTrace);
        return result;
    }
}
