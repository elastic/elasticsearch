/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.spell.LevenshteinDistance;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Defines the different types of query rule criteria and their rules for matching input against the criteria.
 */
public enum QueryRuleCriteriaType {

    ALWAYS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            return true;
        }
    },
    EXACT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            if (input instanceof String && criteriaValue instanceof String) {
                return input.equals(criteriaValue);
            } else {
                return parseDouble(input) == parseDouble(criteriaValue);
            }
        }
    },
    FUZZY {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            final LevenshteinDistance ld = new LevenshteinDistance();
            if (input instanceof String && criteriaValue instanceof String) {
                return ld.getDistance((String) input, (String) criteriaValue) > 0.5f;
            }
            return false;
        }
    },
    PREFIX {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return ((String) input).startsWith((String) criteriaValue);
        }
    },
    SUFFIX {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return ((String) input).endsWith((String) criteriaValue);
        }
    },
    CONTAINS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return ((String) input).contains((String) criteriaValue);
        }
    },
    LT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return parseDouble(input) < parseDouble(criteriaValue);
        }
    },
    LTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return parseDouble(input) <= parseDouble(criteriaValue);
        }
    },
    GT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return parseDouble(input) > parseDouble(criteriaValue);
        }
    },
    GTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validateInput(input);
            return parseDouble(input) >= parseDouble(criteriaValue);
        }
    },
    INFER {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            throw new UnsupportedOperationException("[" + this + "] criteria type requires inference service");
        }

        @Override
        public boolean isMatch(
            QueryRulesInferenceService inferenceService,
            Object input,
            Object criteriaValue,
            Map<String, Object> criteriaProperties
        ) {
            validateInput(input);
            String modelId = criteriaProperties.getOrDefault("model_id", ".elser_model_1").toString();
            String inferenceConfig = criteriaProperties.getOrDefault("inference_config", "text_expansion").toString();
            float threshold = ((Double) criteriaProperties.getOrDefault("threshold", 1.0)).floatValue();
            return inferenceService.findInferenceRuleMatches(modelId, inferenceConfig, (String) input, (String) criteriaValue, threshold);
        }
    };

    public void validateInput(Object input) {
        boolean isValid = isValidForInput(input);
        if (isValid == false) {
            throw new IllegalArgumentException("Input [" + input + "] is not valid for criteria type [" + this + "]");
        }
    }

    public boolean isMatch(
        QueryRulesInferenceService inferenceService,
        Object input,
        Object criteriaValue,
        Map<String, Object> criteriaProperties
    ) {
        throw new UnsupportedOperationException("Inference matches are not supported for criteria type [" + this + "]");
    }

    public abstract boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties);

    public static QueryRuleCriteriaType type(String criteriaType) {
        for (QueryRuleCriteriaType type : values()) {
            if (type.name().equalsIgnoreCase(criteriaType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown QueryRuleCriteriaType: " + criteriaType);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    private boolean isValidForInput(Object input) {
        if (this == EXACT) {
            return input instanceof String || input instanceof Number;
        } else if (List.of(FUZZY, PREFIX, SUFFIX, CONTAINS, INFER).contains(this)) {
            return input instanceof String;
        } else if (List.of(LT, LTE, GT, GTE).contains(this)) {
            try {
                if (input instanceof Number == false) {
                    parseDouble(input.toString());
                }
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    private static double parseDouble(Object input) {
        return (input instanceof Number) ? ((Number) input).doubleValue() : Double.parseDouble(input.toString());
    }
}
