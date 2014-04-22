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
package org.elasticsearch.action.bench;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * A benchmark competitor describes how to run a search benchmark. Multiple competitors may be
 * submitted in a single benchmark request, with their results compared.
 *
 * Competitors are executed in two loops. The outer loop is the iteration loop. The number of times
 * this runs is controlled by the 'iterations' variable.
 *
 * The inner loop is the multiplier loop. This is controlled by the 'multiplier' variable.
 *
 * The level of concurrency pertains to the number of simultaneous searches that may be executed within
 * the inner multiplier loop. Iterations are never run concurrently; they run serially.
 */
public class BenchmarkCompetitor implements Streamable {

    private String name;
    private BenchmarkSettings settings = new BenchmarkSettings();

    /**
     * Constructs a competition across the given indices
     * @param indices   Indices
     */
    BenchmarkCompetitor(String... indices) {
        settings.indices(indices);
    }

    /**
     * Constructs a competition
     */
    BenchmarkCompetitor() { }

    ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (name == null) {
            validationException = ValidateActions.addValidationError("name must not be null", validationException);
        }
        if (settings.concurrency() < 1) {
            validationException = ValidateActions.addValidationError("concurrent requests must be >= 1 but was [" + settings.concurrency() + "]", validationException);
        }
        if (settings.iterations() < 1) {
            validationException = ValidateActions.addValidationError("iterations must be >= 1 but was [" + settings.iterations() + "]", validationException);
        }
        if (settings.multiplier() < 1) {
            validationException = ValidateActions.addValidationError("multiplier must be >= 1 but was [" + settings.multiplier() + "]", validationException);
        }
        if (settings.numSlowest() < 0) {
            validationException = ValidateActions.addValidationError("numSlowest must be >= 0 but was [" + settings.numSlowest() + "]", validationException);
        }
        if (settings.searchType() == null) {
            validationException = ValidateActions.addValidationError("searchType must not be null", validationException);
        }
        return validationException;
    }

    /**
     * Gets the user-supplied name
     * @return  Name
     */
    public String name() {
        return name;
    }

    /**
     * Sets the user-supplied name
     * @param name  Name
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * Gets the benchmark settings
     * @return  Settings
     */
    public BenchmarkSettings settings() {
        return settings;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        settings = in.readOptionalStreamable(new BenchmarkSettings());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalStreamable(settings);
    }
}
