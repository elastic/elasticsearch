/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

public class EnterpriseSearchFeature {
    // Enum of features
    public enum Feature {
        SEARCH_APPLICATION("Search Application"),
        BEHAVIORAL_ANALYTICS("Behavioral Analytics"),
        QUERY_RULE("Query Rule");

        private final String name;

        // Constructor to initialize the feature name
        Feature(String name) {
            this.name = name;
        }

        // Getter method to retrieve the feature name
        public String getName() {
            return name;
        }
    }
}
