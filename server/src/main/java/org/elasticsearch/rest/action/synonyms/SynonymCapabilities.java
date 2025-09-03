/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.synonyms;

import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestPutSynonymsAction} and {@link RestPutSynonymRuleAction}.
 */
public final class SynonymCapabilities {

    private SynonymCapabilities() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static final String SYNONYMS_REFRESH_PARAM = "synonyms_refresh_param";

    public static final Set<String> CAPABILITIES = Set.of(SYNONYMS_REFRESH_PARAM);
}
