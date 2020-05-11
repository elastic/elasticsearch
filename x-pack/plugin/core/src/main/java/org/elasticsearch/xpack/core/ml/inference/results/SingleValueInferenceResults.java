/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class SingleValueInferenceResults implements InferenceResults {

    private final double value;
    private final List<FeatureImportance> featureImportance;

    static List<FeatureImportance> takeTopFeatureImportances(List<FeatureImportance> unsortedFeatureImportances, int numTopFeatures) {
        if (unsortedFeatureImportances == null || unsortedFeatureImportances.isEmpty()) {
            return unsortedFeatureImportances;
        }
        return unsortedFeatureImportances.stream()
            .sorted((l, r)-> Double.compare(Math.abs(r.getImportance()), Math.abs(l.getImportance())))
            .limit(numTopFeatures)
            .collect(Collectors.toList());
    }

    SingleValueInferenceResults(StreamInput in) throws IOException {
        value = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.featureImportance = in.readList(FeatureImportance::new);
        } else {
            this.featureImportance = Collections.emptyList();
        }
    }

    SingleValueInferenceResults(double value, List<FeatureImportance> featureImportance) {
        this.value = value;
        this.featureImportance = featureImportance == null ? Collections.emptyList() : featureImportance;
    }

    public Double value() {
        return value;
    }

    public List<FeatureImportance> getFeatureImportance() {
        return featureImportance;
    }

    public String valueAsString() {
        return String.valueOf(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeList(this.featureImportance);
        }
    }

}
