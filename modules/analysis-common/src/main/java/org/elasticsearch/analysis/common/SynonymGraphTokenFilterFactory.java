/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

public class SynonymGraphTokenFilterFactory extends SynonymTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(SynonymGraphTokenFilterFactory.class);

    public static final String DEPRECATION_MESSAGE = "The [synonym_graph] token filter is deprecated. Use the [synonym] token filter "
        + "instead, which produces the same token graph for indices created on this version or later.";

    SynonymGraphTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        SynonymsManagementAPIService synonymsManagementAPIService,
        CircuitBreaker circuitBreaker
    ) {
        super(indexSettings, env, name, settings, synonymsManagementAPIService, circuitBreaker, false);
        // Older indices get no warning: there [synonym] is still the legacy filter, so switching would change their analysis.
        if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.SYNONYM_FILTER_USES_SYNONYM_GRAPH)) {
            DEPRECATION_LOGGER.warn(DeprecationCategory.ANALYSIS, "synonym_graph_token_filter", DEPRECATION_MESSAGE);
        }
    }
}
