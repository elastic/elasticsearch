/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import java.util.regex.Pattern;

/**
 * Shared naming conventions for PromQL labels represented as ES|QL fields.
 */
public final class PromqlLabels {
    private PromqlLabels() {}

    public static final String PROMETHEUS_LABELS_PREFIX = "labels.";

    /**
     * The classic Prometheus label-name grammar ({@code [a-zA-Z_][a-zA-Z0-9_]*}). This intentionally uses the legacy
     * (non-UTF-8) validation scheme: it is deterministic, admits the reserved {@code __name__} label, and maps cleanly to
     * the passthrough field / identity-blob key names that derived labels are written into.
     */
    private static final Pattern VALID_LABEL_NAME = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    /**
     * Whether {@code name} is a valid PromQL label name under the classic (legacy) grammar. Used to validate the
     * destination and source label names of {@code label_replace}/{@code label_join} at analysis time, matching
     * Prometheus's {@code IsValidLabelName} check.
     */
    public static boolean isValidLabelName(String name) {
        return name != null && VALID_LABEL_NAME.matcher(name).matches();
    }
}
