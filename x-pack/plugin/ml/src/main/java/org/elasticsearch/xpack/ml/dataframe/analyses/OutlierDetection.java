/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class OutlierDetection extends AbstractDataFrameAnalysis {

    public enum Method {
        LOF, LDOF, DISTANCE_KTH_NN, DISTANCE_KNN;

        public static Method fromString(String value) {
            return Method.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final String NUMBER_NEIGHBOURS = "number_neighbours";
    public static final String METHOD = "method";

    private final Integer numberNeighbours;
    private final Method method;

    public OutlierDetection(Integer numberNeighbours, Method method) {
        this.numberNeighbours = numberNeighbours;
        this.method = method;
    }

    @Override
    public Type getType() {
        return Type.OUTLIER_DETECTION;
    }

    @Override
    protected Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        if (numberNeighbours != null) {
            params.put(NUMBER_NEIGHBOURS, numberNeighbours);
        }
        if (method != null) {
            params.put(METHOD, method);
        }
        return params;
    }

    static class Factory implements DataFrameAnalysis.Factory {

        @Override
        public DataFrameAnalysis create(Map<String, Object> config) {
            Integer numberNeighbours = DataFrameAnalysesUtils.readInt(Type.OUTLIER_DETECTION, config, NUMBER_NEIGHBOURS);
            String method = DataFrameAnalysesUtils.readString(Type.OUTLIER_DETECTION, config, METHOD);
            return new OutlierDetection(numberNeighbours, method == null ? null : Method.fromString(method));
        }
    }
}
