/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class SingleValueInferenceResults implements InferenceResults {

    private final double value;
    private final Map<String, Double> featureImportance;

    static Map<String, Double> takeTopFeatureImportances(Map<String, Double> unsortedFeatureImportances, int numTopFeatures) {
        return unsortedFeatureImportances.entrySet()
            .stream()
            .sorted((l, r)-> Double.compare(Math.abs(r.getValue()), Math.abs(l.getValue())))
            .limit(numTopFeatures)
            .collect(LinkedHashMap::new, (h, e) -> h.put(e.getKey(), e.getValue()) , LinkedHashMap::putAll);
    }

    SingleValueInferenceResults(StreamInput in) throws IOException {
        value = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.featureImportance = in.readMap(StreamInput::readString, StreamInput::readDouble);
        } else {
            this.featureImportance = Collections.emptyMap();
        }
    }

    SingleValueInferenceResults(double value, Map<String, Double> featureImportance) {
        this.value = value;
        this.featureImportance = ExceptionsHelper.requireNonNull(featureImportance, "featureImportance");
    }

    public Double value() {
        return value;
    }

    public Map<String, Double> getFeatureImportance() {
        return featureImportance;
    }

    public String valueAsString() {
        return String.valueOf(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeMap(this.featureImportance, StreamOutput::writeString, StreamOutput::writeDouble);
        }
    }

}
