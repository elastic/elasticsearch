/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import java.util.List;
import java.util.Map;

public record QueryRulesAnalysisConfig(String index, String analyzer, String tokenizer, List<String> filters) {

    public static QueryRulesAnalysisConfig fromMap(Map<String, Object> configurationAttributes) {
        String index = (String) configurationAttributes.get("index");
        String analyzer = (String) configurationAttributes.get("analyzer");
        String tokenizer = (String) configurationAttributes.get("tokenizer");
        @SuppressWarnings("unchecked")
        List<String> filters = (List<String>) configurationAttributes.getOrDefault("filters", List.of());
        return new QueryRulesAnalysisConfig(index, analyzer, tokenizer, filters);

    }

}
