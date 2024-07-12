/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.core.Assertions;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * The Contents of this file were originally moved from {@link Version}.
 * <p>
 * This class keeps all the supported OpenSearch predecessor versions for
 * backward compatibility purpose.
 *
 * @opensearch.internal
 */
public class LegacyESVersion extends Version {

    public static final LegacyESVersion V_6_0_0 = new LegacyESVersion(6000099, org.apache.lucene.util.Version.fromBits(7, 0, 0));
    public static final LegacyESVersion V_6_5_0 = new LegacyESVersion(6050099, org.apache.lucene.util.Version.fromBits(7, 0, 0));
    public static final LegacyESVersion V_7_2_0 = new LegacyESVersion(7020099, org.apache.lucene.util.Version.LUCENE_8_0_0);

    // todo move back to Version.java if retiring legacy version support
    protected static final Map<Integer, Version> idToVersion;
    protected static final Map<String, Version> stringToVersion;
    static {
        final Map<Integer, Version> builder = new HashMap<>();
        final Map<String, Version> builderByString = new HashMap<>();

        for (final Field declaredField : LegacyESVersion.class.getFields()) {
            if (declaredField.getType().equals(Version.class) || declaredField.getType().equals(LegacyESVersion.class)) {
                final String fieldName = declaredField.getName();
                if (fieldName.equals("CURRENT") || fieldName.equals("V_EMPTY")) {
                    continue;
                }
                assert fieldName.matches("V_\\d+_\\d+_\\d+(_alpha[1,2]|_beta[1,2]|_rc[1,2])?") : "expected Version field ["
                    + fieldName
                    + "] to match V_\\d+_\\d+_\\d+";
                try {
                    final Version version = (Version) declaredField.get(null);
                    if (Assertions.ENABLED) {
                        final String[] fields = fieldName.split("_");
                        if (fields.length == 5) {
                            assert (fields[1].equals("1") || fields[1].equals("6")) && fields[2].equals("0") : "field "
                                + fieldName
                                + " should not have a build qualifier";
                        } else {
                            final int major = Integer.valueOf(fields[1]) * 1000000;
                            final int minor = Integer.valueOf(fields[2]) * 10000;
                            final int revision = Integer.valueOf(fields[3]) * 100;
                            final int expectedId;
                            if (major > 0 && major < 6000000) {
                                expectedId = 0x08000000 ^ (major + minor + revision + 99);
                            } else {
                                expectedId = (major + minor + revision + 99);
                            }
                            assert version.id == expectedId : "expected version ["
                                + fieldName
                                + "] to have id ["
                                + expectedId
                                + "] but was ["
                                + version.id
                                + "]";
                        }
                    }
                    final Version maybePrevious = builder.put(version.id, version);
                    builderByString.put(version.toString(), version);
                    assert maybePrevious == null : "expected ["
                        + version.id
                        + "] to be uniquely mapped but saw ["
                        + maybePrevious
                        + "] and ["
                        + version
                        + "]";
                } catch (final IllegalAccessException e) {
                    assert false : "Version field [" + fieldName + "] should be public";
                } catch (final RuntimeException e) {
                    assert false : "Version field [" + fieldName + "] threw [" + e + "] during initialization";
                }
            }
        }
        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) : "Version must be upgraded to ["
            + org.apache.lucene.util.Version.LATEST
            + "] is still set to ["
            + CURRENT.luceneVersion
            + "]";

        builder.put(V_EMPTY_ID, V_EMPTY);
        builderByString.put(V_EMPTY.toString(), V_EMPTY);
        idToVersion = Map.copyOf(builder);
        stringToVersion = Map.copyOf(builderByString);
    }

    protected LegacyESVersion(int id, org.apache.lucene.util.Version luceneVersion) {
        // flip the 28th bit of the version id
        // this will be flipped back in the parent class to correctly identify as a legacy version;
        // giving backwards compatibility with legacy systems
        super(id ^ 0x08000000, luceneVersion);
    }


    public boolean isBeta() {
        return major < 5 ? build < 50 : build >= 25 && build < 50;
    }

    /**
     * Returns true iff this version is an alpha version
     * Note: This has been introduced in version 5 of the OpenSearch predecessor. Previous versions will never
     * have an alpha version.
     */

    public boolean isAlpha() {
        return major < 5 ? false : build < 25;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (stringHasLength(version) == false) {
            return Version.CURRENT;
        }
        final Version cached = stringToVersion.get(version);
        if (cached != null) {
            return cached;
        }
        return fromStringSlow(version);
    }

    // pkg private for usage in Version (todo: remove in 3.0)
    static Version fromStringSlow(String version) {
        final boolean snapshot; // this is some BWC for 2.x and before indices
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("[.-]");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                "the version needs to contain major, minor, and revision, and optionally the build: " + version
            );
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            if (rawMajor >= 5 && snapshot) { // we don't support snapshot as part of the version here anymore
                throw new IllegalArgumentException("illegal version format - snapshots are only supported until version 2.x");
            }
            if (rawMajor >= 7 && parts.length == 4) { // we don't support qualifier as part of the version anymore
                throw new IllegalArgumentException("illegal version format - qualifiers are only supported until version 6.x");
            }
            final int betaOffset = rawMajor < 5 ? 0 : 25;
            // we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;

            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("alpha")) {
                    assert rawMajor >= 5 : "major must be >= 5 but was " + major;
                    build = Integer.parseInt(buildStr.substring(5));
                    assert build < 25 : "expected a alpha build but " + build + " >= 25";
                } else if (buildStr.startsWith("Beta") || buildStr.startsWith("beta")) {
                    build = betaOffset + Integer.parseInt(buildStr.substring(4));
                    assert build < 50 : "expected a beta build but " + build + " >= 50";
                } else if (buildStr.startsWith("RC") || buildStr.startsWith("rc")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                } else {
                    throw new IllegalArgumentException("unable to parse version " + version);
                }
            }

            return Version.fromId(major + minor + revision + build);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    /**
     * this is used to ensure the version id for new versions of OpenSearch are always less than the predecessor versions
     */
    protected int maskId(final int id) {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (isAlpha()) {
            sb.append("-alpha");
            sb.append(build);
        } else if (isBeta()) {
            if (major >= 2) {
                sb.append("-beta");
            } else {
                sb.append(".Beta");
            }
            sb.append(major < 5 ? build : build - 25);
        } else if (build < 99) {
            if (major >= 2) {
                sb.append("-rc");
            } else {
                sb.append(".RC");
            }
            sb.append(build - 50);
        }
        return sb.toString();
    }


    protected Version computeMinIndexCompatVersion() {
        final int prevLuceneVersionMajor = this.luceneVersion.major - 1;
        final int bwcMajor;
        if (major == 5) {
            bwcMajor = 2; // we jumped from 2 to 5
        } else if (major == 7) {
            return LegacyESVersion.fromId(6000026);
        } else {
            bwcMajor = major - 1;
        }
        final int bwcMinor = 0;
        return new LegacyESVersion(
            bwcMajor * 1000000 + bwcMinor * 10000 + 99,
            org.apache.lucene.util.Version.fromBits(prevLuceneVersionMajor, 0, 0)
        );
    }
}

