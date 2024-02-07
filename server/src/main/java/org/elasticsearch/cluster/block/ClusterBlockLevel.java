/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.block;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum ClusterBlockLevel {
    READ,
    WRITE,
    METADATA_READ,
    METADATA_WRITE;

    public static final Set<ClusterBlockLevel> ALL = Collections.unmodifiableSet(EnumSet.allOf(ClusterBlockLevel.class));
    public static final Set<ClusterBlockLevel> READ_WRITE = Collections.unmodifiableSet(EnumSet.of(READ, WRITE));
}
