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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A benchmark request contains one or more competitors, which are descriptions of how to
 * perform an individual benchmark. Each competitor has its own settings such as concurrency,
 * number of iterations to perform, and what type of search to perform.
 */
public class BenchmarkRequest extends MasterNodeOperationRequest<BenchmarkRequest> {

    private String benchmarkName;
    private boolean verbose;
    private int numExecutorNodes = 1;   // How many nodes to run the benchmark on
    private double[] percentiles = BenchmarkSettings.DEFAULT_PERCENTILES;

    // Global settings which can be overwritten at the competitor level
    private BenchmarkSettings settings = new BenchmarkSettings();
    private List<BenchmarkCompetitor> competitors = new ArrayList<>();

    /**
     * Constructs a benchmark request
     */
    public BenchmarkRequest() { }

    /**
     * Constructs a benchmark request
     */
    public BenchmarkRequest(String... indices) {
        settings().indices(indices);
    }

    /**
     * Validate benchmark request
     *
     * @return  Null if benchmark request is OK, exception otherwise
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (benchmarkName == null) {
            validationException = ValidateActions.addValidationError("benchmarkName must not be null", validationException);
        }
        if (competitors.isEmpty()) {
            validationException = ValidateActions.addValidationError("competitors must not be empty", validationException);
        }
        if (numExecutorNodes <= 0) {
            validationException = ValidateActions.addValidationError("num_executors must not be less than 1", validationException);
        }
        for (BenchmarkCompetitor competitor : competitors) {
            validationException = competitor.validate(validationException);
            if (validationException != null) {
                break;
            }
        }
        return validationException;
    }

    /**
     * Cascade top-level benchmark settings to individual competitors while taking care
     * not to overwrite any settings which the competitors specifically set.
     */
    public void cascadeGlobalSettings() {
        for (BenchmarkCompetitor competitor : competitors) {
            competitor.settings().merge(settings);
            if (competitor.settings().searchRequests().isEmpty()) {
                for (SearchRequest defaultSearchRequest : settings.searchRequests()) {
                    SearchRequest copy = new SearchRequest();
                    if (defaultSearchRequest.indices() != null) {
                        copy.indices(defaultSearchRequest.indices());
                    }
                    copy.types(defaultSearchRequest.types());
                    copy.searchType(defaultSearchRequest.searchType());
                    copy.source(defaultSearchRequest.source(), true);
                    copy.extraSource(defaultSearchRequest.extraSource(), true);
                    copy.routing(defaultSearchRequest.routing());
                    copy.preference(defaultSearchRequest.preference());
                    competitor.settings().addSearchRequest(copy);
                }
            }
        }
    }

    /**
     * Apply late binding for certain settings. Indices and types passed will override previously
     * set values. Cache clear requests cannot be constructed until we know the final set of
     * indices so do this last.
     *
     * @param indices   List of indices to execute on
     * @param types     List of types to execute on
     */
    public void applyLateBoundSettings(String[] indices, String[] types) {
        if (indices != null && indices.length > 0) {
            settings.indices(indices);
        }
        if (types != null && types.length > 0) {
            settings.types(types);
        }
        for (SearchRequest searchRequest : settings.searchRequests()) {
            if (indices != null && indices.length > 0) {
                searchRequest.indices(indices);
            }
            if (types != null && types.length > 0) {
                searchRequest.types(types);
            }
            if (settings.clearCachesSettings() != null) {
                settings.buildClearCachesRequestFromSettings();
            }
        }
        for (BenchmarkCompetitor competitor : competitors) {
            if (indices != null && indices.length > 0) {
                competitor.settings().indices(indices);
                competitor.settings().types(null);
            }
            if (types != null && types.length > 0) {
                competitor.settings().types(types);
            }
            competitor.settings().buildSearchRequestsFromSettings();
            if (competitor.settings().clearCachesSettings() != null) {
                competitor.settings().buildClearCachesRequestFromSettings();
            }
        }
    }

    /**
     * Gets the benchmark settings
     * @return  Settings
     */
    public BenchmarkSettings settings() {
        return settings;
    }

    /**
     * Gets the number of nodes in the cluster to execute on.
     * @return  Number of nodes
     */
    public int numExecutorNodes() {
        return numExecutorNodes;
    }

    /**
     * Sets the number of nodes in the cluster to execute the benchmark on. We will attempt to
     * execute on this many eligible nodes, but will not fail if fewer nodes are available.
     * @param numExecutorNodes  Number of nodes
     */
    public void numExecutorNodes(int numExecutorNodes) {
        this.numExecutorNodes = numExecutorNodes;
    }

    /**
     * Gets the name of the benchmark
     * @return  Benchmark name
     */
    public String benchmarkName() {
        return benchmarkName;
    }

    /**
     * Sets the name of the benchmark
     * @param benchmarkId   Benchmark name
     */
    public void benchmarkName(String benchmarkId) {
        this.benchmarkName = benchmarkId;
    }

    /**
     * Whether to report detailed statistics
     * @return  True if verbose on
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Whether to report detailed statistics
     * @param verbose   True/false
     */
    public void verbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Gets the list of percentiles to report
     * @return The list of percentiles to report
     */
    public double[] percentiles() {
        return percentiles;
    }

    /**
     * Sets the list of percentiles to report
     * @param percentiles   The list of percentiles to report
     */
    public void percentiles(double[] percentiles) {
        this.percentiles = percentiles;
    }

    /**
     * Gets the list of benchmark competitions
     * @return  Competitions
     */
    public List<BenchmarkCompetitor> competitors() {
        return competitors;
    }

    /**
     * Add a benchmark competition
     * @param competitor    Competition
     */
    public void addCompetitor(BenchmarkCompetitor competitor) {
        this.competitors.add(competitor);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkName = in.readString();
        numExecutorNodes = in.readVInt();
        verbose = in.readBoolean();
        percentiles = in.readDoubleArray();
        int size = in.readVInt();
        competitors.clear();
        for (int i = 0; i < size; i++) {
            BenchmarkCompetitor competitor = new BenchmarkCompetitor();
            competitor.readFrom(in);
            competitors.add(competitor);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(benchmarkName);
        out.writeVInt(numExecutorNodes);
        out.writeBoolean(verbose);
        if (percentiles != null) {
            out.writeDoubleArray(percentiles);
        } else {
            out.writeVInt(0);
        }
        out.writeVInt(competitors.size());
        for (BenchmarkCompetitor competitor : competitors) {
            competitor.writeTo(out);
        }
    }
}
