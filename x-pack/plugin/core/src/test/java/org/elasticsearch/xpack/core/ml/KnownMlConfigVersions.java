/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import java.util.List;

/**
 * Provides access to all known ML config versions
 */
public class KnownMlConfigVersions {
    /**
     * A sorted list of all known ML config versions
     */
    public static final List<MlConfigVersion> ALL_VERSIONS = List.copyOf(MlConfigVersion.getAllVersions());
}
