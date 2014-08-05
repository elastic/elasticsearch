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
package org.elasticsearch.action.benchmark.abort;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to abort a specified benchmark
 */
public class BenchmarkAbortRequest extends MasterNodeOperationRequest<BenchmarkAbortRequest> {

    private String[] benchmarkIdPatterns = Strings.EMPTY_ARRAY;

    public BenchmarkAbortRequest() { }

    public BenchmarkAbortRequest(String... benchmarkIdPatterns) {
        this.benchmarkIdPatterns = benchmarkIdPatterns;
    }

    public void benchmarkIdPatterns(String... benchmarkNames) {
        this.benchmarkIdPatterns = benchmarkNames;
    }

    public String[] benchmarkIdPatterns() {
        return benchmarkIdPatterns;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (benchmarkIdPatterns == null || benchmarkIdPatterns.length == 0) {
            return ValidateActions.addValidationError("benchmark names must not be null or empty", null);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkIdPatterns = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(benchmarkIdPatterns);
    }
}
