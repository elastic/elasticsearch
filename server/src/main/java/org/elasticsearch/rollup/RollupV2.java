/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rollup;

import org.elasticsearch.Build;

public class RollupV2 {
    public static final boolean ROLLUP_V2_FEATURE_FLAG_ENABLED = Build.CURRENT.isSnapshot()
        || "true".equals(System.getProperty("es.rollup_v2_feature_flag_enabled"));

    public static boolean isEnabled() {
        return ROLLUP_V2_FEATURE_FLAG_ENABLED;
    }
}
