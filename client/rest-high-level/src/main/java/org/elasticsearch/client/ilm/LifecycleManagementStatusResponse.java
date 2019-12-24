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

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

/**
 * The current status of index lifecycle management. See {@link OperationMode} for available statuses.
 */
public class LifecycleManagementStatusResponse {

    private final OperationMode operationMode;
    private static final String OPERATION_MODE = "operation_mode";
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<LifecycleManagementStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        OPERATION_MODE, true, a -> new LifecycleManagementStatusResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(OPERATION_MODE));
    }

    //package private for testing
    LifecycleManagementStatusResponse(String operationMode) {
        this.operationMode = OperationMode.fromString(operationMode);
    }

    public OperationMode getOperationMode() {
        return operationMode;
    }

    public static LifecycleManagementStatusResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LifecycleManagementStatusResponse that = (LifecycleManagementStatusResponse) o;
        return operationMode == that.operationMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationMode);
    }
}
