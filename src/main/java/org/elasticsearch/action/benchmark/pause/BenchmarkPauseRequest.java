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

package org.elasticsearch.action.benchmark.pause;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request class for resuming one or more paused benchmarks
 */
public class BenchmarkPauseRequest extends MasterNodeOperationRequest<BenchmarkPauseRequest> {

    private String[] benchmarkIdPatterns = Strings.EMPTY_ARRAY;

    public BenchmarkPauseRequest(String... benchmarkIdPatterns) {
        this.benchmarkIdPatterns = benchmarkIdPatterns;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (benchmarkIdPatterns == null || benchmarkIdPatterns.length == 0) {
            validationException = ValidateActions.addValidationError("Benchmark ID patterns must not be null", validationException);
        }
        return validationException;
    }

    public String[] benchmarkIdPatterns() {
        return benchmarkIdPatterns;
    }

    public void benchmarkIdPatterns(String... benchmarkIds) {
        this.benchmarkIdPatterns = benchmarkIds;
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
