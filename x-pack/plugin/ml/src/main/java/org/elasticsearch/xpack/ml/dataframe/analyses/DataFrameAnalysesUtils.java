/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalysisConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class DataFrameAnalysesUtils {

    private static final Map<DataFrameAnalysis.Type, DataFrameAnalysis.Factory> factories;

    static {
        factories = new HashMap<>();
        factories.put(DataFrameAnalysis.Type.OUTLIER_DETECTION, new OutlierDetection.Factory());
    }

    private DataFrameAnalysesUtils() {}

    public static List<DataFrameAnalysis> readAnalyses(List<DataFrameAnalysisConfig> analyses) {
        return analyses.stream().map(DataFrameAnalysesUtils::readAnalysis).collect(Collectors.toList());
    }

    static DataFrameAnalysis readAnalysis(DataFrameAnalysisConfig config) {
        Map<String, Object> configMap = config.asMap();
        DataFrameAnalysis.Type analysisType = DataFrameAnalysis.Type.fromString(configMap.keySet().iterator().next());
        DataFrameAnalysis.Factory factory = factories.get(analysisType);
        if (factory == null) {
            throw new ElasticsearchParseException("Unknown analysis type [{}]", analysisType);
        }
        Map<String, Object> analysisConfig = castAsMapAndCopy(analysisType, configMap.get(analysisType.toString()));
        DataFrameAnalysis dataFrameAnalysis = factory.create(analysisConfig);
        if (analysisConfig.isEmpty() == false) {
            throw new ElasticsearchParseException("Data frame analysis [{}] does not support one or more provided parameters: {}",
                analysisType, analysisConfig.keySet());
        }
        return dataFrameAnalysis;
    }

    private static Map<String, Object> castAsMapAndCopy(DataFrameAnalysis.Type analysisType, Object obj) {
        try {
            return new HashMap<>((Map<String, Object>) obj);
        } catch (ClassCastException e) {
            throw new ElasticsearchParseException("[{}] expected to be a map but was of type [{}]", analysisType, obj.getClass().getName());
        }
    }

    @Nullable
    static Integer readInt(DataFrameAnalysis.Type analysisType, Map<String, Object> config, String property) {
        Object value = config.remove(property);
        if (value == null) {
            return null;
        }
        try {
            return (int) value;
        } catch (ClassCastException e) {
            throw new ElasticsearchParseException("Property [{}] of analysis [{}] should be of type int but was [{}]",
                property, analysisType, value.getClass().getName());
        }
    }

    @Nullable
    static String readString(DataFrameAnalysis.Type analysisType, Map<String, Object> config, String property) {
        Object value = config.remove(property);
        if (value == null) {
            return null;
        }
        try {
            return (String) value;
        } catch (ClassCastException e) {
            throw new ElasticsearchParseException("Property [{}] of analysis [{}] should be of string int but was [{}]",
                property, analysisType, value.getClass().getName());
        }
    }
}
