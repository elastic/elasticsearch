/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

public class DataLifecycleConfigurationService {

    public static final boolean FEATURE_FLAG_ENABLED = "true".equals(System.getProperty("es.dlm_feature_flag_enabled"));

    public static boolean isEnabled() {
        return FEATURE_FLAG_ENABLED;
    }
}
