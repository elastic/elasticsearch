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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParsedPCAStats extends BaseParsedMatrixStats {
    /** list of field names */
    protected List<String> fieldNames;

    private List<ParsedPCAStatsResults> eigenResults;
    /** eigen values from PCA */
    protected final Map<String, Complex_F64> eigenVals = new HashMap<>();
    /** eigen vectors from PCA */
    protected final Map<String, DMatrixRMaj> eigenVectors = new HashMap<>();

    @Override
    public String getType() {
        return PCAAggregationBuilder.NAME;
    }

    protected void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
        for (String field : fieldNames) {
            eigenVals.put(field, new Complex_F64());
            eigenVectors.put(field, new DMatrixRMaj());
        }
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
        Set<String> fields = counts.keySet().size() == 0 ? new HashSet<>(fieldNames) : counts.keySet();
        for (String fieldName : fields) {
            builder.value(fieldName);
        }
        builder.endArray();
        builder.startArray(Fields.PRINCIPAL_COMPONENT);
        for (String fieldName : fields) {
            builder.startObject();
            builder.field(Fields.EIGENVALUE, getEigenValue(fieldName).toString());
            builder.field(Fields.EIGENVECTOR, getEigenVector(fieldName).data);
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
        PARSER.declareStringArray(ParsedPCAStats::setFieldNames, new ParseField(Fields.FIELDS));
        PCA_PARSER.declareString(ParsedPCAStatsResults::setEigenValue, new ParseField(Fields.EIGENVALUE));
        PCA_PARSER.declareDoubleArray(ParsedPCAStatsResults::setEigenVector, new ParseField(Fields.EIGENVECTOR));
        PARSER.declareObjectArray((pcaStats, results) -> {
            pcaStats.eigenResults = results;
        }, (p, c) -> ParsedPCAStatsResults.fromXContent(p), new ParseField(Fields.PRINCIPAL_COMPONENT));
    }

    public static ParsedPCAStats fromXContent(XContentParser parser, String name) throws IOException {
        ParsedPCAStats stats = PARSER.parse(parser, null);
        int i = 0;
        for (String field : stats.fieldNames) {
            stats.eigenVals.put(field, stats.eigenResults.get(i).eigenValue);
            stats.eigenVectors.put(field, stats.eigenResults.get(i++).eigenVector);
        }
        stats.setName(name);
        return stats;
    }


    static class ParsedPCAStatsResults {
        Complex_F64 eigenValue;
        DMatrixRMaj eigenVector;

        public void setEigenValue(String value) {
            String[] split = value.split("\\s+");
            double r = Double.parseDouble(split[0]);
            double i = 0;
            if (split.length > 1) {
                i = Double.parseDouble(split[1].split("i")[0]);
            }
            eigenValue = new Complex_F64(r, i);
        }

        public void setEigenVector(List<Double> vector) {
            double[] vals = new double[vector.size()];
            for (int i = 0; i < vals.length; ++i) {
                vals[i] = vector.get(i);
            }
            eigenVector = new DMatrixRMaj(vals);
        }

        public static ParsedPCAStatsResults fromXContent(XContentParser parser) {
            ParsedPCAStatsResults pcaStats = PCA_PARSER.apply(parser, null);
            return pcaStats;
        }
    }
}
