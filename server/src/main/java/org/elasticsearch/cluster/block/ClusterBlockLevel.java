/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.block;

import java.util.EnumSet;

public enum ClusterBlockLevel {
    READ,
    WRITE,
    METADATA_READ,
    METADATA_WRITE,
    REFRESH;

    public static final EnumSet<ClusterBlockLevel> ALL = EnumSet.allOf(ClusterBlockLevel.class);
    public static final EnumSet<ClusterBlockLevel> READ_WRITE = EnumSet.of(READ, WRITE);
}
