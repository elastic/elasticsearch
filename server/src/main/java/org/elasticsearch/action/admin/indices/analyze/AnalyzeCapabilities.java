/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.analyze;

import java.util.Set;

public final class AnalyzeCapabilities {
    private AnalyzeCapabilities() {}

    private static final String WRONG_CUSTOM_ANALYZER_RETURNS_400_CAPABILITY = "wrong_custom_analyzer_returns_400";

    public static final Set<String> CAPABILITIES = Set.of(WRONG_CUSTOM_ANALYZER_RETURNS_400_CAPABILITY);
}
