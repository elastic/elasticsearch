/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;

import java.util.NavigableSet;

public final class BWCVersions {
    private BWCVersions() {}

    public static NavigableSet<TransportVersion> getAllBWCVersions() {
        return TransportVersionUtils.allReleasedVersions().tailSet(TransportVersions.MINIMUM_COMPATIBLE, true);
    }

    public static final NavigableSet<TransportVersion> DEFAULT_BWC_VERSIONS = getAllBWCVersions();
}
