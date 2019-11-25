/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AcknowledgedTasksResponse {

    public static final ParseField TASK_FAILURES = new ParseField("task_failures");
    public static final ParseField NODE_FAILURES = new ParseField("node_failures");

    @SuppressWarnings("unchecked")
    protected static <T extends AcknowledgedTasksResponse> ConstructingObjectParser<T, Void> generateParser(
            String name,
            TriFunction<Boolean, List<TaskOperationFailure>, List<? extends ElasticsearchException>, T> ctor,
            String ackFieldName) {

        ConstructingObjectParser<T, Void> parser = new ConstructingObjectParser<>(name, true,
                args -> ctor.apply((boolean) args[0], (List<TaskOperationFailure>) args[1], (List<ElasticsearchException>) args[2]));
        parser.declareBoolean(constructorArg(), new ParseField(ackFieldName));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p), TASK_FAILURES);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), NODE_FAILURES);
        return parser;
    }

    private boolean acknowledged;
    private List<TaskOperationFailure> taskFailures;
    private List<ElasticsearchException> nodeFailures;

    public AcknowledgedTasksResponse(boolean acknowledged, @Nullable List<TaskOperationFailure> taskFailures,
                                     @Nullable List<? extends ElasticsearchException> nodeFailures) {
        this.acknowledged = acknowledged;
        this.taskFailures = taskFailures == null ? Collections.emptyList() : Collections.unmodifiableList(taskFailures);
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : Collections.unmodifiableList(nodeFailures);
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AcknowledgedTasksResponse other = (AcknowledgedTasksResponse) obj;
        return acknowledged == other.acknowledged
                && taskFailures.equals(other.taskFailures)
                && nodeFailures.equals(other.nodeFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, taskFailures, nodeFailures);
    }
}
