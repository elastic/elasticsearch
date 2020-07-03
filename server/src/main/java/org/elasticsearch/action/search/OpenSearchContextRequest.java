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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class OpenSearchContextRequest extends ActionRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final TimeValue keepAlive;

    @Nullable
    private final String routing;
    @Nullable
    private final String preference;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    public OpenSearchContextRequest(String[] indices, IndicesOptions indicesOptions,
                                    TimeValue keepAlive, String routing, String preference) {
        this.indices = Objects.requireNonNull(indices);
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.keepAlive = keepAlive;
        this.routing = routing;
        this.preference = preference;
    }

    public OpenSearchContextRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.keepAlive = in.readTimeValue();
        this.routing = in.readOptionalString();
        this.preference = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(keepAlive);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices.length == 0) {
            validationException = addValidationError("[index] is not specified", validationException);
        }
        if (keepAlive == null) {
            validationException = addValidationError("[keep_alive] is not specified", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public OpenSearchContextRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    TimeValue keepAlive() {
        return keepAlive;
    }

    String routing() {
        return routing;
    }

    String preference() {
        return preference;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "open search context: indices [" + String.join(",", indices) + "] keep_alive [" + keepAlive + "]";
            }
        };
    }
}
