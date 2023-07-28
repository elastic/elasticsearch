/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import java.util.List;

/**
 * Provides access to all known index versions
 */
public class KnownIndexVersions {
    /**
     * A sorted list of all known transport versions
     */
    public static final List<IndexVersion> ALL_VERSIONS = List.copyOf(IndexVersion.getAllVersions());
}
