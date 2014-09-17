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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The result of a benchmark competition. Maintains per-node results.
 */
public class CompetitionResult implements Streamable, ToXContent {

    private String competitionName;
    private int concurrency;
    private int multiplier;
    private boolean verbose;
    private double[] percentiles = BenchmarkSettings.DEFAULT_PERCENTILES;

    private List<CompetitionNodeResult> nodeResults = new ArrayList<>();

    private CompetitionSummary competitionSummary;
    private CompetitionDetails competitionDetails;

    public CompetitionResult() { }

    /**
     * Constructs a competition result
     * @param competitionName   Competition name
     * @param concurrency       Concurrency
     * @param multiplier        Internal multiplier; each iteration will run this many times to smooth out measurements
     * @param percentiles       Which percentiles to report on
     */
    public CompetitionResult(String competitionName, int concurrency, int multiplier, double[] percentiles) {
        this(competitionName, concurrency, multiplier, false, percentiles);
    }

    /**
     * Constructs a competition result
     * @param competitionName   Competition name
     * @param concurrency       Concurrency
     * @param multiplier        Internal multiplier; each iteration will run this many times to smooth out measurements
     * @param verbose           Whether to report detailed statistics
     * @param percentiles       Which percentiles to report on
     */
    public CompetitionResult(String competitionName, int concurrency, int multiplier, boolean verbose, double[] percentiles) {
        this.competitionName = competitionName;
        this.concurrency = concurrency;
        this.multiplier = multiplier;
        this.verbose = verbose;
        this.percentiles = (percentiles != null && percentiles.length > 0) ? percentiles : BenchmarkSettings.DEFAULT_PERCENTILES;
        this.competitionDetails = new CompetitionDetails(nodeResults);
        this.competitionSummary = new CompetitionSummary(nodeResults, concurrency, multiplier, percentiles);
    }

    /**
     * Adds a node-level competition result
     * @param nodeResult    Node result
     */
    public void addCompetitionNodeResult(CompetitionNodeResult nodeResult) {
        nodeResults.add(nodeResult);
    }

    /**
     * Gets detailed statistics for the competition
     * @return  Detailed statistics
     */
    public CompetitionDetails competitionDetails() {
        return competitionDetails;
    }

    /**
     * Gets summary statistics for the competition
     * @return  Summary statistics
     */
    public CompetitionSummary competitionSummary() {
        return competitionSummary;
    }

    /**
     * Gets the name of the competition
     * @return  Name
     */
    public String competitionName() {
        return competitionName;
    }

    /**
     * Gets the concurrency level; determines how many threads will be executing the competition concurrently.
     * @return  Concurrency
     */
    public int concurrency() {
        return concurrency;
    }

    /**
     * Gets the multiplier. The multiplier determines how many times each iteration will be run.
     * @return  Multiplier
     */
    public int multiplier() {
        return multiplier;
    }

    /**
     * Whether to report detailed statistics
     * @return  True/false
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets whether to report detailed statistics
     * @param verbose   True/false
     */
    public void verbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Gets list of percentiles to report
     * @return  List of percentiles
     */
    public double[] percentiles() {
        return percentiles;
    }

    /**
     * Sets the list of percentiles to report
     * @param percentiles   Percentiles
     */
    public void percentiles(double[] percentiles) {
        this.percentiles = percentiles;
    }

    /**
     * Gets the results for each cluster node that the competition executed on.
     * @return  Node-level results
     */
    public List<CompetitionNodeResult> nodeResults() {
        return nodeResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(competitionName);
        competitionSummary.toXContent(builder, params);
        if (verbose) {
            competitionDetails.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        competitionName = in.readString();
        concurrency = in.readVInt();
        multiplier = in.readVInt();
        verbose = in.readBoolean();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            CompetitionNodeResult result = new CompetitionNodeResult();
            result.readFrom(in);
            nodeResults.add(result);
        }
        percentiles = in.readDoubleArray();
        competitionSummary = new CompetitionSummary(nodeResults, concurrency, multiplier, percentiles);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(competitionName);
        out.writeVInt(concurrency);
        out.writeVInt(multiplier);
        out.writeBoolean(verbose);
        out.writeVInt(nodeResults.size());
        for (CompetitionNodeResult result : nodeResults) {
            result.writeTo(out);
        }
        out.writeDoubleArray(percentiles);
    }
}
