/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.ejml.data.Complex_F64;
import org.ejml.data.DMatrixRMaj;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.matrix.stats.BaseParsedMatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.InternalMatrixStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParsedPCAStats extends BaseParsedMatrixStats {
    /** eigen values from PCA */
    protected final Map<String, Complex_F64> eigenVals = new HashMap<>();
    /** eigen vectors from PCA */
    protected final Map<String, DMatrixRMaj> eigenVectors = new HashMap<>();

    @Override
    public String getType() {
        return PCAAggregationBuilder.NAME;
    }

    public Complex_F64 getEigenValue(String field) {
        return checkedGet(eigenVals, field);
    }

    public DMatrixRMaj getEigenVector(String field) {
        return checkedGet(eigenVectors, field);
    }

    public static class Fields extends InternalMatrixStats.Fields {
        public static final String PRINCIPAL_COMPONENT = "pc";
        public static final String EIGENVALUE = "eigenvalue";
        public static final String EIGENVECTOR = "eigenvector";
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        builder.startArray(Fields.FIELDS);
        for (String fieldName : counts.keySet()) {
            builder.value(fieldName);
        }
        builder.endArray();
        builder.startArray(Fields.PRINCIPAL_COMPONENT);
        for (String fieldName : counts.keySet()) {
            builder.startObject();
            builder.field(Fields.EIGENVALUE, getEigenValue(fieldName));
            builder.field(Fields.EIGENVECTOR, getEigenVector(fieldName));
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    private static ObjectParser<ParsedPCAStats, Void> PARSER =
        new ObjectParser<>(ParsedPCAStats.class.getSimpleName(), true, ParsedPCAStats::new);

    private static ObjectParser<ParsedPCAStatsResults, Void> PCA_PARSER =
        new ObjectParser<>(ParsedPCAStatsResults.class.getSimpleName(), true, ParsedPCAStatsResults::new);

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong(ParsedPCAStats::setDocCount, CommonFields.DOC_COUNT);
        PCA_PARSER.declareStringArray(ParsedPCAStatsResults::setFieldNames, new ParseField(Fields.FIELDS));
        PCA_PARSER.declareObjectArray((pcaStats, results) -> {

        }, (p, c) -> ParsedPCAStatsResults.fromXContent(p), new ParseField(Fields.PRINCIPAL_COMPONENT));
    }

    public static ParsedPCAStats fromXContent(XContentParser parser, String name) throws IOException {
        ParsedPCAStats aggregation = PARSER.parse(parser, null);
        ParsedPCAStatsResults results = PCA_PARSER.parse(parser, null);
        aggregation.setName(name);
        // IN WORK!
        return aggregation;
    }


    static class ParsedPCAStatsResults {
        List<String> fieldNames;


        public ParsedPCAStatsResults setFieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public static ParsedPCAStats fromXContent(XContentParser parser) {
            ParsedPCAStats pcaStats = PARSER.apply(parser, null);
            return pcaStats;
        }
    }
}
