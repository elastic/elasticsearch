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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 *
 */
public final class MergeMappingException extends MapperException {

    private final String[] failures;

    public MergeMappingException(String[] failures) {
        super("Merge failed with failures {" + Arrays.toString(failures) + "}");
        Objects.requireNonNull(failures, "failures must be non-null");
        this.failures = failures;
    }

    public MergeMappingException(StreamInput in) throws IOException {
        super(in);
        failures = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(failures);
    }

    public String[] failures() {
        return failures;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
