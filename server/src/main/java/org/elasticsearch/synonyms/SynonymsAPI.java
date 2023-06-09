/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.util.FeatureFlag;

public class SynonymsAPI {
    private static final FeatureFlag SYNONYMS_API_FEATURE_FLAG = new FeatureFlag("synonyms_api");

    public static boolean isEnabled() {
        return SYNONYMS_API_FEATURE_FLAG.isEnabled();
    }
}
