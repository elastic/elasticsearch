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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Detailed statistics for each iteration of a benchmark search competition.
 */
public class CompetitionDetails implements ToXContent {

    private List<CompetitionNodeResult> nodeResults;

    public CompetitionDetails(List<CompetitionNodeResult> nodeResults) {
        this.nodeResults = nodeResults;
    }

    /**
     * Gets node-level competition results
     * @return  A list of node-level competition results
     */
    public List<CompetitionNodeResult> getNodeResults() {
        return nodeResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        final int highestIteration = highestCompletedIteration();
        computeAllStatistics();
        CompetitionIteration prototypical = prototypicalIteration();

        builder.startObject(Fields.ITERATIONS);

        for (int i = 0; i < highestIteration; i++) {

            builder.field(Fields.ITERATION, i);
            builder.startObject();

            builder.startObject(Fields.MIN);
            for (CompetitionNodeResult nodeResult : nodeResults) {
                CompetitionIteration iteration = nodeResult.iterations().get(i);
                builder.field(nodeResult.nodeName(), iteration == null ? Fields.NULL : iteration.min());
            }
            builder.endObject();
            builder.startObject(Fields.MAX);
            for (CompetitionNodeResult nodeResult : nodeResults) {
                CompetitionIteration iteration = nodeResult.iterations().get(i);
                builder.field(nodeResult.nodeName(), iteration == null ? Fields.NULL : iteration.max());
            }
            builder.endObject();
            builder.startObject(Fields.MEAN);
            for (CompetitionNodeResult nodeResult : nodeResults) {
                CompetitionIteration iteration = nodeResult.iterations().get(i);
                builder.field(nodeResult.nodeName(), iteration == null ? Fields.NULL : iteration.mean());
            }
            builder.endObject();
            builder.startObject(Fields.TOTAL_TIME);
            for (CompetitionNodeResult nodeResult : nodeResults) {
                CompetitionIteration iteration = nodeResult.iterations().get(i);
                builder.field(nodeResult.nodeName(), iteration == null ? Fields.NULL : iteration.totalTime());
            }
            builder.endObject();
            builder.startObject(Fields.QPS);
            for (CompetitionNodeResult nodeResult : nodeResults) {
                CompetitionIteration iteration = nodeResult.iterations().get(i);
                builder.field(nodeResult.nodeName(), iteration == null ? Fields.NULL : iteration.queriesPerSecond());
            }
            builder.endObject();

            // All nodes track the same percentiles, so just choose a prototype to iterate
            if (prototypical != null) {
                for (Map.Entry<Double, Double> entry : prototypical.percentileValues().entrySet()) {
                    // Change back to integral value for display purposes
                    builder.startObject(new XContentBuilderString("percentile_" + entry.getKey().longValue()));

                    for (CompetitionNodeResult nodeResult : nodeResults) {
                        CompetitionIteration iteration = nodeResult.iterations().get(i);
                        if (iteration != null) {
                            Double value = iteration.percentileValues().get(entry.getKey());
                            builder.field(nodeResult.nodeName(), (value.isNaN()) ? 0.0 : value);
                        } else {
                            builder.field(nodeResult.nodeName(), Fields.NULL);
                        }
                    }
                    builder.endObject();
                }
            }

            builder.endObject();
        }
        return builder;
    }

    /**
     * Calculates detailed statistics for each iteration. Should be called prior to
     * accessing individual measurements.
     */
    public void computeAllStatistics() {
        for (CompetitionNodeResult nodeResult : nodeResults) {
            for (CompetitionIteration iteration : nodeResult.iterations()) {
                iteration.computeStatistics();
            }
        }
    }

    private CompetitionIteration prototypicalIteration() {
        if (nodeResults != null && nodeResults.size() > 0) {
            CompetitionNodeResult nodeResult = nodeResults.get(0);
            if (nodeResult.iterations() != null && nodeResult.iterations().size() > 0) {
                return nodeResult.iterations().get(0);
            }
        }
        return null;
    }

    private int highestCompletedIteration() {
        int count = 0;
        for (CompetitionNodeResult nodeResult : nodeResults) {
            count = Math.max(count, nodeResult.completedIterations());
        }
        return count;
    }

    static final class Fields {
        static final XContentBuilderString ITERATIONS = new XContentBuilderString("iterations");
        static final XContentBuilderString ITERATION = new XContentBuilderString("iteration");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString QPS = new XContentBuilderString("qps");
        static final XContentBuilderString NULL = new XContentBuilderString("null");
    }
}
