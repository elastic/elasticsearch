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
import org.elasticsearch.core.Tuple;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TransportVersionUtils {
    private static final List<TransportVersion> ALL_VERSIONS;

    static {
//        Tuple<List<Version>, List<Version>> versions = resolveReleasedVersions(Version.CURRENT, Version.class);
//        RELEASED_VERSIONS = versions.v1();
//        UNRELEASED_VERSIONS = versions.v2();
//        List<Version> allVersions = new ArrayList<>(RELEASED_VERSIONS.size() + UNRELEASED_VERSIONS.size());
//        allVersions.addAll(RELEASED_VERSIONS);
//        allVersions.addAll(UNRELEASED_VERSIONS);
//        Collections.sort(allVersions);
        ALL_VERSIONS = getDeclaredVersions();//Collections.emptyList();//Collections.unmodifiableList(allVersions);
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /**
     * Extracts a sorted list of declared version constants from a class.
     * The argument would normally be Version.class but is exposed for
     * testing with other classes-containing-version-constants.
     */
    public static List<TransportVersion> getDeclaredVersions() {
        final Field[] fields = TransportVersion.class.getFields();
        final List<TransportVersion> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                continue;
            }
            if (field.getType() != TransportVersion.class) {
                continue;
            }
            switch (field.getName()) {
                case "CURRENT":
                case "V_EMPTY":
                    continue;
            }
            assert field.getName().matches("V(_\\d+){3}?") || field.getName().equals("ZERO") || field.getName().equals("MINIMUM_COMPATIBLE") : field.getName();
            try {
                versions.add(((TransportVersion) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        return versions;
    }

    public static TransportVersion getPreviousVersion() {
        TransportVersion version = getPreviousVersion(TransportVersion.CURRENT);
        assert version.before(TransportVersion.CURRENT);
        return version;
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        for (int i = ALL_VERSIONS.size() - 1; i >= 0; i--) {//shoudl be RELEASED_VERSIONS..
            TransportVersion v = ALL_VERSIONS.get(i);
            if (v.before(version)) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
    }
}
