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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;

/**
 * The current status of index lifecycle management. See {@link OperationMode} for available statuses.
 */
public class StatusILMResponse {

    private final OperationMode operationMode;
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<StatusILMResponse, Void> PARSER = new ConstructingObjectParser<>(
        "operation_mode", a -> new StatusILMResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("operation_mode"));
    }

    //package private for testing
    StatusILMResponse(String operationMode) {
        this.operationMode = OperationMode.fromString(operationMode);
    }

    public OperationMode getOperationMode() {
        return operationMode;
    }

    public static StatusILMResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Enum representing the different modes that Index Lifecycle Service can operate in.
     */
    public enum OperationMode {
        /**
         * This represents a state where no policies are executed
         */
        STOPPED,

        /**
         * this represents a state where only sensitive actions (like {@link ShrinkAction}) will be executed
         * until they finish, at which point the operation mode will move to <code>STOPPED</code>.
         */
        STOPPING,

        /**
         * Normal operation where all policies are executed as normal.
         */
        RUNNING;

        static OperationMode fromString(String string) {
            return EnumSet.allOf(OperationMode.class).stream()
                .filter(e -> string.equalsIgnoreCase(e.name())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(Locale.ENGLISH, "%s is not a valid operation_mode", string)));
        }
    }

    // generated
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatusILMResponse that = (StatusILMResponse) o;
        return operationMode == that.operationMode;
    }

    // generated
    @Override
    public int hashCode() {
        return Objects.hash(operationMode);
    }
}
