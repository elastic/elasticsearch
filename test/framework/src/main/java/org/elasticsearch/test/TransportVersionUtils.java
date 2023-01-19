/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;

import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportVersionUtils {
    private static final NavigableSet<TransportVersion> ALL_VERSIONS;

    static {
        ALL_VERSIONS = TransportVersion.getAllVersions();
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion() {
        return ESTestCase.randomFrom(ALL_VERSIONS);
    }

    /** Returns a random {@link TransportVersion} from all available versions without the ignore set */
    public static TransportVersion randomVersion(Set<TransportVersion> ignore) {
        return ESTestCase.randomFrom(ALL_VERSIONS.stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    public static TransportVersion getPreviousVersion() {
        TransportVersion version = getPreviousVersion(TransportVersion.CURRENT);
        assert version.before(TransportVersion.CURRENT);
        return version;
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        TransportVersion prev = ALL_VERSIONS.lower(version);
        if (prev == null) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return prev;
    }
}
