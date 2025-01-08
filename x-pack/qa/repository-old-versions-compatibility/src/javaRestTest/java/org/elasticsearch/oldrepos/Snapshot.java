/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

/**
 * This is a mapping to the created snapshot zip files, covering test cases for "old repositories"
 */
public enum Snapshot {

    FIVE("5", "Index created in vES_5 - Basic mapping"),
    FIVE_CUSTOM_ANALYZER("5_custom_analyzer", "Index created in vES_5 - Custom-Analyzer - standard token filter"),
    SIX("6", "Index created in vES_6 - Basic mapping"),
    SIX_CUSTOM_ANALYZER("6_custom_analyzer", "Index created in vES_6 - Custom-Analyzer - standard token filter");

    private final String name;
    // The description is for debugging
    private final String description;

    Snapshot(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
