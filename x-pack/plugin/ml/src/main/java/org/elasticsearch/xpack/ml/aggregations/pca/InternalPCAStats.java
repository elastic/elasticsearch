/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.matrix.stats.BaseInternalMatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.InternalMatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsResults;
import org.elasticsearch.search.aggregations.matrix.stats.RunningStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalPCAStats extends BaseInternalMatrixStats implements MatrixStats {
    /** per shard ctor */
    InternalPCAStats(String name, long count, RunningStats multiFieldStatsResults, MatrixStatsResults results,
                     List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, count, multiFieldStatsResults, results, pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalPCAStats(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected PCAStatsResults readOptionalWriteableMatrixStatsResults(StreamInput in) throws IOException {
        return in.readOptionalWriteable(PCAStatsResults::new);
    }

    @Override
    public String getWriteableName() {
        return PCAAggregationBuilder.NAME;
    }

    @Override
    public InternalPCAStats newInternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults,
                                                   MatrixStatsResults results, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalPCAStats(name, count, multiFieldStatsResults, results, pipelineAggregators, metaData);
    }

    @Override
    public PCAStatsResults newMatrixStatsResults(RunningStats runningStats) {
        return new PCAStatsResults(runningStats);
    }

    @Override
    public PCAStatsResults newMatrixStatsResults() {
        return new PCAStatsResults();
    }

    public PCAStatsResults getResults() {
        return (PCAStatsResults)results;
    }

    public static class Fields extends BaseInternalMatrixStats.Fields {
        public static final String PRINCIPAL_COMPONENTS = "pc";
        public static final String EIGENVALUE = "eigenvalue";
        public static final String EIGENVECTOR = "eigenvector";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        PCAStatsResults results = getResults();
        if (results != null && results.getFieldCounts().keySet().isEmpty() == false) {
            // put fields
            builder.startArray(InternalMatrixStats.Fields.FIELDS);
            for (String fieldName : results.getFieldCounts().keySet()) {
                builder.value(fieldName);
            }
            builder.endArray();

            // put principal components
            builder.startArray(Fields.PRINCIPAL_COMPONENTS);
            for (String fieldName : results.getFieldCounts().keySet()) {
                builder.startObject();
                builder.field(Fields.EIGENVALUE, results.getEigenValue(fieldName).toString());
                double[] eigenVec = results.getEigenVector(fieldName);
                builder.array(Fields.EIGENVECTOR, eigenVec);
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }
}
