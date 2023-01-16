/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportVersionUtils {
    private static final List<TransportVersion> ALL_VERSIONS;

    static {
        ALL_VERSIONS = List.copyOf(TransportVersion.getDeclaredVersions());
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion() {
        return ESTestCase.randomFrom(ALL_VERSIONS);
    }

    /** Returns a random {@link TransportVersion} from all available versions without the ignore set */
    public static TransportVersion randomVersion(Set<TransportVersion> ignore) {
        return ESTestCase.randomFrom(ALL_VERSIONS.stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /** Returns a random {@link Version} from all available versions. */
    public static TransportVersion randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    public static TransportVersion getPreviousVersion() {
        TransportVersion version = getPreviousVersion(TransportVersion.CURRENT);
        assert version.before(TransportVersion.CURRENT);
        return version;
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        int place = Collections.binarySearch(ALL_VERSIONS, version);
        if (place < 0) {
            // version does not exist - need the item before the index this version should be inserted
            place = -(place + 1);
        }
        if (place <= 1) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return ALL_VERSIONS.get(place - 1);
    }
}
