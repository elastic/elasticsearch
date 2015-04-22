/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** Utilities for selecting versions in tests */
public class VersionUtils {

    private static final List<Version> SORTED_VERSIONS;
    static {
        Field[] declaredFields = Version.class.getDeclaredFields();
        Set<Integer> ids = new HashSet<>();
        for (Field field : declaredFields) {
            final int mod = field.getModifiers();
            if (Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                if (field.getType() == Version.class) {
                    try {
                        Version object = (Version) field.get(null);
                        ids.add(object.id);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        List<Integer> idList = new ArrayList<>(ids);
        Collections.sort(idList);
        ImmutableList.Builder<Version> version = ImmutableList.builder();
        for (Integer integer : idList) {
            version.add(Version.fromId(integer));
        }
        SORTED_VERSIONS = version.build();
    }

    /** Returns immutable list of all known versions. */
    public static List<Version> allVersions() {
        return Collections.unmodifiableList(SORTED_VERSIONS);
    }
    
    /** Returns the {@link Version} before the {@link Version#CURRENT} */
    public static Version getPreviousVersion() {
        Version version = SORTED_VERSIONS.get(SORTED_VERSIONS.size() - 2);
        assert version.before(Version.CURRENT);
        return version;
    }
    
    /** Returns the oldest {@link Version} */
    public static Version getFirstVersion() {
        return SORTED_VERSIONS.get(0);
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return SORTED_VERSIONS.get(random.nextInt(SORTED_VERSIONS.size()));
    }

    /** Returns a random {@link Version} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static Version randomVersionBetween(Random random, Version minVersion, Version maxVersion) {
        int minVersionIndex = 0;
        if (minVersion != null) {
            minVersionIndex = SORTED_VERSIONS.indexOf(minVersion);
        }
        int maxVersionIndex = SORTED_VERSIONS.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = SORTED_VERSIONS.indexOf(maxVersion);
        }
        if (minVersionIndex == -1) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex == -1) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else if (minVersionIndex > maxVersionIndex) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = maxVersionIndex + 1 - minVersionIndex;
            return SORTED_VERSIONS.get(minVersionIndex + random.nextInt(range));
        }
    }
}
