/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

public interface MlConfigVersionExtension {
    /**
     * Returns the {@link MlConfigVersion} that Elasticsearch should use.
     * <p>
     * This must be at least equal to the latest version found in {@link MlConfigVersion} V_* constants.
     * @param fallback   The latest version from server
     */
    MlConfigVersion getCurrentMlConfigVersion(MlConfigVersion fallback);

}
