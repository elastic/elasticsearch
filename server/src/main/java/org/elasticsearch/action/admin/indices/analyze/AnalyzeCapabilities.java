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

    private static final String MAX_CHAR_COUNT_CAPABILITY = "analyze_max_char_count";

    /**
     * Thai analysis ({@code thai} analyzer and tokenizer, {@code _thai_} stopwords) follows the index's creation version: indices
     * created before {@code IndexVersions.THAI_ANALYSIS_LUCENE_10_6} keep the Lucene 10.5 output, newer indices and index-less
     * requests get Lucene's current output.
     */
    private static final String THAI_ANALYSIS_BY_INDEX_VERSION_CAPABILITY = "thai_analysis_by_index_version";

    public static final Set<String> CAPABILITIES = Set.of(
        WRONG_CUSTOM_ANALYZER_RETURNS_400_CAPABILITY,
        MAX_CHAR_COUNT_CAPABILITY,
        THAI_ANALYSIS_BY_INDEX_VERSION_CAPABILITY
    );
}
