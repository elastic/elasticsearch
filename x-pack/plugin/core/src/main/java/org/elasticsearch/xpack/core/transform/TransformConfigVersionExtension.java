/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

public interface TransformConfigVersionExtension {
    /**
     * Returns the {@link TransformConfigVersion} that Elasticsearch should use.
     * <p>
     * This must be at least equal to the latest version found in {@link TransformConfigVersion} V_* constants.
     * @param fallback   The latest version from server
     */
    TransformConfigVersion getCurrentTransformConfigVersion(TransformConfigVersion fallback);
}
