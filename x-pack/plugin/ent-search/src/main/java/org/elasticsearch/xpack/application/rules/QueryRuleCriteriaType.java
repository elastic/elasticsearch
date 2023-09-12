/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Defines the different types of query rule criteria and their rules for matching input against the criteria.
 */
public enum QueryRuleCriteriaType {

    ALWAYS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return true;
        }
    },
    EXACT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
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
            validate(input, criteriaProperties);
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
            validate(input, criteriaProperties);
            return ((String) input).startsWith((String) criteriaValue);
        }
    },
    SUFFIX {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return ((String) input).endsWith((String) criteriaValue);
        }
    },
    CONTAINS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return ((String) input).contains((String) criteriaValue);
        }
    },
    LT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return parseDouble(input) < parseDouble(criteriaValue);
        }
    },
    LTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return parseDouble(input) <= parseDouble(criteriaValue);
        }
    },
    GT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return parseDouble(input) > parseDouble(criteriaValue);
        }
    },
    GTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue, Map<String, Object> criteriaProperties) {
            validate(input, criteriaProperties);
            return parseDouble(input) >= parseDouble(criteriaValue);
        }
    },
    INFER {

        private final String PROPERTY_MODEL_ID = "model_id";
        private final String PROPERTY_INFERENCE_CONFIG = "inference_config";
        private final String PROPERTY_THRESHOLD = "threshold";
        private final Map<String, Object> DEFAULTS = Map.of(
            PROPERTY_MODEL_ID,
            ".elser_model_1",
            PROPERTY_INFERENCE_CONFIG,
            TextExpansionConfig.NAME,
            PROPERTY_THRESHOLD,
            1.0
        );

        @Override
        public void validateProperties(Map<String, Object> criteriaProperties) {
            for (String propertyName : criteriaProperties.keySet()) {
                if (DEFAULTS.containsKey(propertyName) == false) {
                    throw new IllegalArgumentException(
                        "Unsupported property ["
                            + propertyName
                            + "] for criteria type ["
                            + this
                            + "]. Allowed properties: "
                            + DEFAULTS.keySet()
                    );
                }
            }
            if (criteriaProperties.containsKey(PROPERTY_INFERENCE_CONFIG)) {
                String inferenceConfig = (String) criteriaProperties.get(PROPERTY_INFERENCE_CONFIG);
                if (QueryRulesInferenceService.SUPPORTED_INFERENCE_CONFIGS.contains(inferenceConfig) == false) {
                    throw new IllegalArgumentException(
                        "Unsupported inference config ["
                            + inferenceConfig
                            + "]. Supported inference configs: "
                            + QueryRulesInferenceService.SUPPORTED_INFERENCE_CONFIGS
                    );
                }
            }
        }

        @Override
        public Set<String> getAllowedPropertyNames() {
            return DEFAULTS.keySet();
        }

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
            validate(input, criteriaProperties);
            String modelId = criteriaProperties.getOrDefault(PROPERTY_MODEL_ID, DEFAULTS.get(PROPERTY_MODEL_ID)).toString();
            String inferenceConfig = criteriaProperties.getOrDefault(PROPERTY_INFERENCE_CONFIG, DEFAULTS.get(PROPERTY_INFERENCE_CONFIG))
                .toString();
            float threshold = ((Double) criteriaProperties.getOrDefault(PROPERTY_THRESHOLD, DEFAULTS.get(PROPERTY_THRESHOLD))).floatValue();
            return inferenceService.findInferenceRuleMatches(modelId, inferenceConfig, (String) input, (String) criteriaValue, threshold);
        }
    };

    public void validate(Object input, Map<String, Object> properties) {
        validateProperties(properties);
        if (isValidForInput(input) == false) {
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

    public void validateProperties(Map<String, Object> criteriaProperties) {
        // Default case: No supported properties.
        if (criteriaProperties.isEmpty() == false) {
            throw new IllegalArgumentException("Criteria type [" + this + "] does not define any allowed properties");
        }
        ;
    }

    public Set<String> getAllowedPropertyNames() {
        return Set.of();
    }

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
