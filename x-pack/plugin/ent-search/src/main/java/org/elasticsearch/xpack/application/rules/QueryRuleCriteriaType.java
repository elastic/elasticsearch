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

/**
 * Defines the different types of query rule criteria and their rules for matching input against the criteria.
 */
public enum QueryRuleCriteriaType {
    ALWAYS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return true;
        }
    },
    EXACT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            if (input instanceof String && criteriaValue instanceof String) {
                return input.equals(criteriaValue);
            } else {
                return parseDouble(input) == parseDouble(criteriaValue);
            }
        }
    },
    FUZZY {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            final LevenshteinDistance ld = new LevenshteinDistance();
            if (input instanceof String && criteriaValue instanceof String) {
                return ld.getDistance((String) input, (String) criteriaValue) > 0.5f;
            }
            return false;
        }
    },
    PREFIX {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return ((String) input).startsWith((String) criteriaValue);
        }
    },
    SUFFIX {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return ((String) input).endsWith((String) criteriaValue);
        }
    },
    CONTAINS {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return ((String) input).contains((String) criteriaValue);
        }
    },
    LT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return parseDouble(input) < parseDouble(criteriaValue);
        }
    },
    LTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return parseDouble(input) <= parseDouble(criteriaValue);
        }
    },
    GT {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            return parseDouble(input) > parseDouble(criteriaValue);
        }
    },
    GTE {
        @Override
        public boolean isMatch(Object input, Object criteriaValue) {
            validateInput(input);
            return parseDouble(input) >= parseDouble(criteriaValue);
        }
    };

    public boolean validateInput(Object input, boolean throwOnInvalidInput) {
        boolean isValid = isValidForInput(input);
        if (isValid == false && throwOnInvalidInput) {
            throw new IllegalArgumentException("Input [" + input + "] is not valid for CriteriaType [" + this + "]");
        }
        return isValid;
    }

    public boolean validateInput(Object input) {
        return validateInput(input, true);
    }

    public abstract boolean isMatch(Object input, Object criteriaValue);

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
        } else if (List.of(FUZZY, PREFIX, SUFFIX, CONTAINS).contains(this)) {
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
