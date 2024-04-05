/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

public enum BehaviorFlag {
    MV_MEDIAN_RETURNS_DOUBLE(Component.ANALYZER);

    public enum Component {
        // We'll consider more components as needed.
        ANALYZER;
    }

    private Component component;

    BehaviorFlag(Component component) {
        this.component = component;
    }

    public Component component() {
        return component;
    }
}
