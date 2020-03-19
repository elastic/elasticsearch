/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class Parameters implements ToXContentObject {

    public static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    public static final ParseField METHOD = new ParseField("method");
    public static final ParseField FEATURE_INFLUENCE_THRESHOLD = new ParseField("feature_influence_threshold");
    public static final ParseField COMPUTE_FEATURE_INFLUENCE = new ParseField("compute_feature_influence");
    public static final ParseField OUTLIER_FRACTION = new ParseField("outlier_fraction");
    public static final ParseField STANDARDIZATION_ENABLED = new ParseField("standardization_enabled");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<Parameters, Void> PARSER = new ConstructingObjectParser<>("outlier_detection_parameters",
        true,
        a -> new Parameters(
            (int) a[0],
            (String) a[1],
            (boolean) a[2],
            (double) a[3],
            (double) a[4],
            (boolean) a[5]
        ));

    static {
        PARSER.declareInt(constructorArg(), N_NEIGHBORS);
        PARSER.declareStringArray(constructorArg(), METHOD);
        PARSER.declareBoolean(constructorArg(), COMPUTE_FEATURE_INFLUENCE);
        PARSER.declareDouble(constructorArg(), FEATURE_INFLUENCE_THRESHOLD);
        PARSER.declareDouble(constructorArg(), OUTLIER_FRACTION);
        PARSER.declareBoolean(constructorArg(), STANDARDIZATION_ENABLED);
    }

    private final int nNeighbors;
    private final String method;
    private final boolean computeFeatureInfluence;
    private final double featureInfluenceThreshold;
    private final double outlierFraction;
    private final boolean standardizationEnabled;

    public Parameters(int nNeighbors, String method, boolean computeFeatureInfluence, double featureInfluenceThreshold,
                      double outlierFraction, boolean standardizationEnabled) {
        this.nNeighbors = nNeighbors;
        this.method = method;
        this.computeFeatureInfluence = computeFeatureInfluence;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
        this.outlierFraction = outlierFraction;
        this.standardizationEnabled = standardizationEnabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(N_NEIGHBORS.getPreferredName(), nNeighbors);
        builder.field(METHOD.getPreferredName(), method);
        builder.field(COMPUTE_FEATURE_INFLUENCE.getPreferredName(), computeFeatureInfluence);
        builder.field(FEATURE_INFLUENCE_THRESHOLD.getPreferredName(), featureInfluenceThreshold);
        builder.field(OUTLIER_FRACTION.getPreferredName(), outlierFraction);
        builder.field(STANDARDIZATION_ENABLED.getPreferredName(), standardizationEnabled);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Parameters that = (Parameters) o;
        return nNeighbors == that.nNeighbors
            && Objects.equals(method, that.method)
            && computeFeatureInfluence == that.computeFeatureInfluence
            && featureInfluenceThreshold == that.featureInfluenceThreshold
            && outlierFraction == that.outlierFraction
            && standardizationEnabled == that.standardizationEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nNeighbors, method, computeFeatureInfluence, featureInfluenceThreshold, outlierFraction,
            standardizationEnabled);
    }
}
