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

package org.elasticsearch;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;

/**
 */
public class Version {
    /*
     * The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is alpha/beta/rc indicator AA
     * values below 25 are for alpha builder (since 5.0), and above 25 and below 50 are beta builds, and below 99 are RC builds, with 99
     * indicating a release the (internal) format of the id is there so we can easily do after/before checks on the id
     */
    public static final int V_2_0_0_beta1_ID = 2000001;
    public static final Version V_2_0_0_beta1 = new Version(V_2_0_0_beta1_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_0_0_beta2_ID = 2000002;
    public static final Version V_2_0_0_beta2 = new Version(V_2_0_0_beta2_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_0_0_rc1_ID = 2000051;
    public static final Version V_2_0_0_rc1 = new Version(V_2_0_0_rc1_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_0_0_ID = 2000099;
    public static final Version V_2_0_0 = new Version(V_2_0_0_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_0_1_ID = 2000199;
    public static final Version V_2_0_1 = new Version(V_2_0_1_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_0_2_ID = 2000299;
    public static final Version V_2_0_2 = new Version(V_2_0_2_ID, org.apache.lucene.util.Version.LUCENE_5_2_1);
    public static final int V_2_1_0_ID = 2010099;
    public static final Version V_2_1_0 = new Version(V_2_1_0_ID, org.apache.lucene.util.Version.LUCENE_5_3_1);
    public static final int V_2_1_1_ID = 2010199;
    public static final Version V_2_1_1 = new Version(V_2_1_1_ID, org.apache.lucene.util.Version.LUCENE_5_3_1);
    public static final int V_2_1_2_ID = 2010299;
    public static final Version V_2_1_2 = new Version(V_2_1_2_ID, org.apache.lucene.util.Version.LUCENE_5_3_1);
    public static final int V_2_2_0_ID = 2020099;
    public static final Version V_2_2_0 = new Version(V_2_2_0_ID, org.apache.lucene.util.Version.LUCENE_5_4_1);
    public static final int V_2_2_1_ID = 2020199;
    public static final Version V_2_2_1 = new Version(V_2_2_1_ID, org.apache.lucene.util.Version.LUCENE_5_4_1);
    public static final int V_2_2_2_ID = 2020299;
    public static final Version V_2_2_2 = new Version(V_2_2_2_ID, org.apache.lucene.util.Version.LUCENE_5_4_1);
    public static final int V_2_3_0_ID = 2030099;
    public static final Version V_2_3_0 = new Version(V_2_3_0_ID, org.apache.lucene.util.Version.LUCENE_5_5_0);
    public static final int V_2_3_1_ID = 2030199;
    public static final Version V_2_3_1 = new Version(V_2_3_1_ID, org.apache.lucene.util.Version.LUCENE_5_5_0);
    public static final int V_2_3_2_ID = 2030299;
    public static final Version V_2_3_2 = new Version(V_2_3_2_ID, org.apache.lucene.util.Version.LUCENE_5_5_0);
    public static final int V_2_3_3_ID = 2030399;
    public static final Version V_2_3_3 = new Version(V_2_3_3_ID, org.apache.lucene.util.Version.LUCENE_5_5_0);
    public static final int V_5_0_0_alpha1_ID = 5000001;
    public static final Version V_5_0_0_alpha1 = new Version(V_5_0_0_alpha1_ID, org.apache.lucene.util.Version.LUCENE_6_0_0);
    public static final int V_5_0_0_alpha2_ID = 5000002;
    public static final Version V_5_0_0_alpha2 = new Version(V_5_0_0_alpha2_ID, org.apache.lucene.util.Version.LUCENE_6_0_0);
    public static final int V_5_0_0_alpha3_ID = 5000003;
    public static final Version V_5_0_0_alpha3 = new Version(V_5_0_0_alpha3_ID, org.apache.lucene.util.Version.LUCENE_6_0_0);
    public static final int V_5_0_0_alpha4_ID = 5000004;
    public static final Version V_5_0_0_alpha4 = new Version(V_5_0_0_alpha4_ID, org.apache.lucene.util.Version.LUCENE_6_1_0);
    public static final Version CURRENT = V_5_0_0_alpha4;

    static {
        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) : "Version must be upgraded to ["
                + org.apache.lucene.util.Version.LATEST + "] is still set to [" + CURRENT.luceneVersion + "]";
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        switch (id) {
            case V_5_0_0_alpha4_ID:
                return V_5_0_0_alpha4;
            case V_5_0_0_alpha3_ID:
                return V_5_0_0_alpha3;
            case V_5_0_0_alpha2_ID:
                return V_5_0_0_alpha2;
            case V_5_0_0_alpha1_ID:
                return V_5_0_0_alpha1;
            case V_2_3_3_ID:
                return V_2_3_3;
            case V_2_3_2_ID:
                return V_2_3_2;
            case V_2_3_1_ID:
                return V_2_3_1;
            case V_2_3_0_ID:
                return V_2_3_0;
            case V_2_2_2_ID:
                return V_2_2_2;
            case V_2_2_1_ID:
                return V_2_2_1;
            case V_2_2_0_ID:
                return V_2_2_0;
            case V_2_1_2_ID:
                return V_2_1_2;
            case V_2_1_1_ID:
                return V_2_1_1;
            case V_2_1_0_ID:
                return V_2_1_0;
            case V_2_0_2_ID:
                return V_2_0_2;
            case V_2_0_1_ID:
                return V_2_0_1;
            case V_2_0_0_ID:
                return V_2_0_0;
            case V_2_0_0_rc1_ID:
                return V_2_0_0_rc1;
            case V_2_0_0_beta2_ID:
                return V_2_0_0_beta2;
            case V_2_0_0_beta1_ID:
                return V_2_0_0_beta1;
            default:
                return new Version(id, org.apache.lucene.util.Version.LATEST);
        }
    }

    /**
     * Return the {@link Version} of Elasticsearch that has been used to create an index given its settings.
     *
     * @throws IllegalStateException if the given index settings doesn't contain a value for the key
     *         {@value IndexMetaData#SETTING_VERSION_CREATED}
     */
    public static Version indexCreated(Settings indexSettings) {
        final Version indexVersion = indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null);
        if (indexVersion == null) {
            throw new IllegalStateException(
                    "[" + IndexMetaData.SETTING_VERSION_CREATED + "] is not present in the index settings for index with uuid: ["
                            + indexSettings.get(IndexMetaData.SETTING_INDEX_UUID) + "]");
        }
        return indexVersion;
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the smallest version between the 2.
     */
    public static Version smallest(Version version1, Version version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (!Strings.hasLength(version)) {
            return Version.CURRENT;
        }
        final boolean snapshot; // this is some BWC for 2.x and before indices
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("\\.|\\-");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                    "the version needs to contain major, minor, and revision, and optionally the build: " + version);
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            if (rawMajor >= 5 && snapshot) { // we don't support snapshot as part of the version here anymore
                throw new IllegalArgumentException("illegal version format - snapshots are only supported until version 2.x");
            }
            final int betaOffset = rawMajor < 5 ? 0 : 25;
            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;


            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("alpha")) {
                    assert rawMajor >= 5 : "major must be >= 5 but was " + major;
                    build = Integer.parseInt(buildStr.substring(5));
                    assert build < 25 : "expected a beta build but " + build + " >= 25";
                } else if (buildStr.startsWith("Beta") || buildStr.startsWith("beta")) {
                    build = betaOffset + Integer.parseInt(buildStr.substring(4));
                    assert build < 50 : "expected a beta build but " + build + " >= 50";
                } else if (buildStr.startsWith("RC") || buildStr.startsWith("rc")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                } else {
                    throw new IllegalArgumentException("unable to parse version " + version);
                }
            }

            return fromId(major + minor + revision + build);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final org.apache.lucene.util.Version luceneVersion;

    Version(int id, org.apache.lucene.util.Version luceneVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.luceneVersion = luceneVersion;
    }

    public boolean after(Version version) {
        return version.id < id;
    }

    public boolean onOrAfter(Version version) {
        return version.id <= id;
    }

    public boolean before(Version version) {
        return version.id > id;
    }

    public boolean onOrBefore(Version version) {
        return version.id >= id;
    }

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public Version minimumCompatibilityVersion() {
        return Version.smallest(this, fromId(major * 1000000 + 99));
    }

    @SuppressForbidden(reason = "System.out.*")
    public static void main(String[] args) {
        System.out.println("Version: " + Version.CURRENT + ", Build: " + Build.CURRENT.shortHash() + "/" + Build.CURRENT.date() + ", JVM: "
                + JvmInfo.jvmInfo().version());
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
            sb.append(major < 5 ? build : build-25);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public boolean isBeta() {
        return major < 5 ? build < 50 : build >= 25 && build < 50;
    }

    /**
     * Returns true iff this version is an alpha version
     * Note: This has been introduced in elasticsearch version 5. Previous versions will never
     * have an alpha version.
     */
    public boolean isAlpha() {
        return major < 5 ? false :  build < 25;
    }

    public boolean isRC() {
        return build > 50 && build < 99;
    }

    public static class Module extends AbstractModule {

        private final Version version;

        public Module(Version version) {
            this.version = version;
        }

        @Override
        protected void configure() {
            bind(Version.class).toInstance(version);
        }
    }
}
