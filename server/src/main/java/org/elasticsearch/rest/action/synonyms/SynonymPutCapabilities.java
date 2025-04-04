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
public final class SynonymPutCapabilities {

    private SynonymPutCapabilities() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static final String PUT_SYNONYMS_TIMEOUT = "put_synonyms_timeout";

    public static final Set<String> CAPABILITIES = Set.of(
        PUT_SYNONYMS_TIMEOUT
    );
}
