/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.boosts;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map;

public abstract class ScriptScoreBoost {
    public enum OperationType {
        multiply,
        add
    }

    protected String type;
    protected OperationType operation = OperationType.multiply;

    protected ScriptScoreBoost(String type, String operation) {
        this.type = type;
        this.operation = OperationType.valueOf(operation);
    }

    public OperationType getOperation() {
        return operation;
    }

    public String getType() {
        return type;
    }

    public abstract String getSource(String field);

    public boolean isAdditive() {
        return this.operation == OperationType.add;
    }

    public boolean isMultiplicative() {
        return this.operation == OperationType.multiply;
    }

    protected String constantFactor() {
        if (isAdditive()) {
            return "0";
        }
        return "1";
    }

    protected String safeValue(String field) {
        return format("(doc[''{0}''].size() > 0) ? doc[''{0}''].value : {1}", field, constantFactor());
    }

    protected String format(String pattern, Object... arguments) {
        MessageFormat formatter = new MessageFormat(pattern, Locale.ROOT);
        return formatter.format(arguments);
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
