/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import java.util.Map;

public abstract class ScriptScoreBoost {
    protected String type;

    protected ScriptScoreBoost(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static ScriptScoreBoost parse(Map<String, Object> props) {
        switch ((String) props.get("type")) {
            case ValueBoost.TYPE:
                return new ValueBoost(
                    (String) props.get("value"),
                    (String) props.get("operation"),
                    Float.parseFloat(props.get("factor").toString())
                );
            case FunctionalBoost.TYPE:
                return new FunctionalBoost(
                    (String) props.get("function"),
                    (String) props.get("operation"),
                    Float.parseFloat(props.get("factor").toString())
                );
            case ProximityBoost.TYPE:
                return new ProximityBoost(
                    (String) props.get("center"),
                    (String) props.get("function"),
                    Float.parseFloat(props.get("factor").toString())
                );
        }
        throw new IllegalArgumentException("Unrecognized boost type: " + props.get("type"));
    }
}
