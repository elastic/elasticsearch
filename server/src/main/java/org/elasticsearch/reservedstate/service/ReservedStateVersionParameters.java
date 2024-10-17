/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;

/**
 * Version parameters including the version {@link ReservedStateVersion} itself and whether re-processing on the same version is accepted.
 */
public record ReservedStateVersionParameters(ReservedStateVersion version, Boolean reprocessSameVersion) {

    public static final ReservedStateVersionParameters EMPTY_VERSION = new ReservedStateVersionParameters(
        ReservedStateMetadata.EMPTY_VERSION,
        false
    );

    public ReservedStateVersionParameters(Long version, Boolean reprocessSameVersion) {
        this(new ReservedStateVersion(version, Version.CURRENT), reprocessSameVersion);
    }

}
