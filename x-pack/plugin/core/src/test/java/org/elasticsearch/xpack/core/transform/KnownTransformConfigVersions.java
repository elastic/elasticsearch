/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import java.util.List;

/**
 * Provides access to all known transform config versions
 */
public class KnownTransformConfigVersions {
    /**
     * A sorted list of all known transform config versions
     */
    public static final List<TransformConfigVersion> ALL_VERSIONS = List.copyOf(TransformConfigVersion.getAllVersions());
}
