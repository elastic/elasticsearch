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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.io.Serializable;

/**
 */
@SuppressWarnings("deprecation")
public class Version implements Serializable {

    // The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is Beta/RC indicator
    // AA values below 50 are beta builds, and below 99 are RC builds, with 99 indicating a release
    // the (internal) format of the id is there so we can easily do after/before checks on the id

    public static final int V_0_18_0_ID = /*00*/180099;
    public static final Version V_0_18_0 = new Version(V_0_18_0_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_1_ID = /*00*/180199;
    public static final Version V_0_18_1 = new Version(V_0_18_1_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_2_ID = /*00*/180299;
    public static final Version V_0_18_2 = new Version(V_0_18_2_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_3_ID = /*00*/180399;
    public static final Version V_0_18_3 = new Version(V_0_18_3_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_4_ID = /*00*/180499;
    public static final Version V_0_18_4 = new Version(V_0_18_4_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_5_ID = /*00*/180599;
    public static final Version V_0_18_5 = new Version(V_0_18_5_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_6_ID = /*00*/180699;
    public static final Version V_0_18_6 = new Version(V_0_18_6_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_7_ID = /*00*/180799;
    public static final Version V_0_18_7 = new Version(V_0_18_7_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_18_8_ID = /*00*/180899;
    public static final Version V_0_18_8 = new Version(V_0_18_8_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_19_0_RC1_ID = /*00*/190051;
    public static final Version V_0_19_0_RC1 = new Version(V_0_19_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_19_0_RC2_ID = /*00*/190052;
    public static final Version V_0_19_0_RC2 = new Version(V_0_19_0_RC2_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_19_0_RC3_ID = /*00*/190053;
    public static final Version V_0_19_0_RC3 = new Version(V_0_19_0_RC3_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_19_0_ID = /*00*/190099;
    public static final Version V_0_19_0 = new Version(V_0_19_0_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_1_ID = /*00*/190199;
    public static final Version V_0_19_1 = new Version(V_0_19_1_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_2_ID = /*00*/190299;
    public static final Version V_0_19_2 = new Version(V_0_19_2_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_3_ID = /*00*/190399;
    public static final Version V_0_19_3 = new Version(V_0_19_3_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_4_ID = /*00*/190499;
    public static final Version V_0_19_4 = new Version(V_0_19_4_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_5_ID = /*00*/190599;
    public static final Version V_0_19_5 = new Version(V_0_19_5_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_6_ID = /*00*/190699;
    public static final Version V_0_19_6 = new Version(V_0_19_6_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_7_ID = /*00*/190799;
    public static final Version V_0_19_7 = new Version(V_0_19_7_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_8_ID = /*00*/190899;
    public static final Version V_0_19_8 = new Version(V_0_19_8_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_9_ID = /*00*/190999;
    public static final Version V_0_19_9 = new Version(V_0_19_9_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_10_ID = /*00*/191099;
    public static final Version V_0_19_10 = new Version(V_0_19_10_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_11_ID = /*00*/191199;
    public static final Version V_0_19_11 = new Version(V_0_19_11_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_12_ID = /*00*/191299;
    public static final Version V_0_19_12 = new Version(V_0_19_12_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_19_13_ID = /*00*/191399;
    public static final Version V_0_19_13 = new Version(V_0_19_13_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_20_0_RC1_ID = /*00*/200051;
    public static final Version V_0_20_0_RC1 = new Version(V_0_20_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_0_ID = /*00*/200099;
    public static final Version V_0_20_0 = new Version(V_0_20_0_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_1_ID = /*00*/200199;
    public static final Version V_0_20_1 = new Version(V_0_20_1_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_2_ID = /*00*/200299;
    public static final Version V_0_20_2 = new Version(V_0_20_2_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_3_ID = /*00*/200399;
    public static final Version V_0_20_3 = new Version(V_0_20_3_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_4_ID = /*00*/200499;
    public static final Version V_0_20_4 = new Version(V_0_20_4_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_5_ID = /*00*/200599;
    public static final Version V_0_20_5 = new Version(V_0_20_5_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_6_ID = /*00*/200699;
    public static final Version V_0_20_6 = new Version(V_0_20_6_ID, false, org.apache.lucene.util.Version.LUCENE_36);
    public static final int V_0_20_7_ID = /*00*/200799;
    public static final Version V_0_20_7 = new Version(V_0_20_7_ID, false, org.apache.lucene.util.Version.LUCENE_36);

    public static final int V_0_90_0_Beta1_ID = /*00*/900001;
    public static final Version V_0_90_0_Beta1 = new Version(V_0_90_0_Beta1_ID, false, org.apache.lucene.util.Version.LUCENE_41);
    public static final int V_0_90_0_RC1_ID = /*00*/900051;
    public static final Version V_0_90_0_RC1 = new Version(V_0_90_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_41);
    public static final int V_0_90_0_RC2_ID = /*00*/900052;
    public static final Version V_0_90_0_RC2 = new Version(V_0_90_0_RC2_ID, false, org.apache.lucene.util.Version.LUCENE_42);
    public static final int V_0_90_0_ID = /*00*/900099;
    public static final Version V_0_90_0 = new Version(V_0_90_0_ID, false, org.apache.lucene.util.Version.LUCENE_42);
    public static final int V_0_90_1_ID = /*00*/900199;
    public static final Version V_0_90_1 = new Version(V_0_90_1_ID, false, org.apache.lucene.util.Version.LUCENE_43);
    public static final int V_0_90_2_ID = /*00*/900299;
    public static final Version V_0_90_2 = new Version(V_0_90_2_ID, false, org.apache.lucene.util.Version.LUCENE_43);
    public static final int V_0_90_3_ID = /*00*/900399;
    public static final Version V_0_90_3 = new Version(V_0_90_3_ID, false, org.apache.lucene.util.Version.LUCENE_44);
    public static final int V_0_90_4_ID = /*00*/900499;
    public static final Version V_0_90_4 = new Version(V_0_90_4_ID, false, org.apache.lucene.util.Version.LUCENE_44);
    public static final int V_0_90_5_ID = /*00*/900599;
    public static final Version V_0_90_5 = new Version(V_0_90_5_ID, false, org.apache.lucene.util.Version.LUCENE_44);
    public static final int V_0_90_6_ID = /*00*/900699;
    public static final Version V_0_90_6 = new Version(V_0_90_6_ID, false, org.apache.lucene.util.Version.LUCENE_45);
    public static final int V_0_90_7_ID = /*00*/900799;
    public static final Version V_0_90_7 = new Version(V_0_90_7_ID, false, org.apache.lucene.util.Version.LUCENE_45);
    public static final int V_0_90_8_ID = /*00*/900899;
    public static final Version V_0_90_8 = new Version(V_0_90_8_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_9_ID = /*00*/900999;
    public static final Version V_0_90_9 = new Version(V_0_90_9_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_10_ID = /*00*/901099;
    public static final Version V_0_90_10 = new Version(V_0_90_10_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_11_ID = /*00*/901199;
    public static final Version V_0_90_11 = new Version(V_0_90_11_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_12_ID = /*00*/901299;
    public static final Version V_0_90_12 = new Version(V_0_90_12_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_13_ID = /*00*/901399;
    public static final Version V_0_90_13 = new Version(V_0_90_13_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_0_90_14_ID = /*00*/901499;
    public static final Version V_0_90_14 = new Version(V_0_90_14_ID, false, org.apache.lucene.util.Version.LUCENE_46);

    public static final int V_1_0_0_Beta1_ID = /*00*/1000001;
    public static final Version V_1_0_0_Beta1 = new Version(V_1_0_0_Beta1_ID, false, org.apache.lucene.util.Version.LUCENE_45);
    public static final int V_1_0_0_Beta2_ID = /*00*/1000002;
    public static final Version V_1_0_0_Beta2 = new Version(V_1_0_0_Beta2_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_0_RC1_ID = /*00*/1000051;
    public static final Version V_1_0_0_RC1 = new Version(V_1_0_0_RC1_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_0_RC2_ID = /*00*/1000052;
    public static final Version V_1_0_0_RC2 = new Version(V_1_0_0_RC2_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_0_ID = /*00*/1000099;
    public static final Version V_1_0_0 = new Version(V_1_0_0_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_1_ID = /*00*/1000199;
    public static final Version V_1_0_1 = new Version(V_1_0_1_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_2_ID = /*00*/1000299;
    public static final Version V_1_0_2 = new Version(V_1_0_2_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_3_ID = /*00*/1000399;
    public static final Version V_1_0_3 = new Version(V_1_0_3_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_0_4_ID = /*00*/1000499;
    public static final Version V_1_0_4 = new Version(V_1_0_3_ID, false, org.apache.lucene.util.Version.LUCENE_46);
    public static final int V_1_1_0_ID = /*00*/1010099;
    public static final Version V_1_1_0 = new Version(V_1_1_0_ID, false, org.apache.lucene.util.Version.LUCENE_47);
    public static final int V_1_1_1_ID = /*00*/1010199;
    public static final Version V_1_1_1 = new Version(V_1_1_1_ID, false, org.apache.lucene.util.Version.LUCENE_47);
    public static final int V_1_1_2_ID = /*00*/1010299;
    public static final Version V_1_1_2 = new Version(V_1_1_2_ID, false, org.apache.lucene.util.Version.LUCENE_47);
    public static final int V_1_2_0_ID = /*00*/1020099;
    public static final Version V_1_2_0 = new Version(V_1_2_0_ID, false, org.apache.lucene.util.Version.LUCENE_48);
    public static final int V_1_2_1_ID = /*00*/1020199;
    public static final Version V_1_2_1 = new Version(V_1_2_1_ID, false, org.apache.lucene.util.Version.LUCENE_48);
    public static final int V_1_2_2_ID = /*00*/1020299;
    public static final Version V_1_2_2 = new Version(V_1_2_2_ID, true, org.apache.lucene.util.Version.LUCENE_48);

    public static final Version CURRENT = V_1_2_2;

    static {
        assert CURRENT.luceneVersion == Lucene.VERSION : "Version must be upgraded to [" + Lucene.VERSION + "] is still set to [" + CURRENT.luceneVersion + "]";
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        switch (id) {
            case V_1_2_2_ID:
                return V_1_2_2;
            case V_1_2_1_ID:
                return V_1_2_1;
            case V_1_2_0_ID:
                return V_1_2_0;
            case V_1_1_2_ID:
                return V_1_1_2;
            case V_1_1_1_ID:
                return V_1_1_1;
            case V_1_1_0_ID:
                return V_1_1_0;
            case V_1_0_4_ID:
                return V_1_0_4;
            case V_1_0_3_ID:
                return V_1_0_3;
            case V_1_0_2_ID:
                return V_1_0_2;
            case V_1_0_1_ID:
                return V_1_0_1;
            case V_1_0_0_ID:
                return V_1_0_0;
            case V_1_0_0_RC2_ID:
                return V_1_0_0_RC2;
            case V_1_0_0_RC1_ID:
                return V_1_0_0_RC1;
            case V_1_0_0_Beta2_ID:
                return V_1_0_0_Beta2;
            case V_1_0_0_Beta1_ID:
                return V_1_0_0_Beta1;
            case V_0_90_14_ID:
                return V_0_90_14;
            case V_0_90_13_ID:
                return V_0_90_13;
            case V_0_90_12_ID:
                return V_0_90_12;
            case V_0_90_11_ID:
                return V_0_90_11;
            case V_0_90_10_ID:
                return V_0_90_10;
            case V_0_90_9_ID:
                return V_0_90_9;
            case V_0_90_8_ID:
                return V_0_90_8;
            case V_0_90_7_ID:
                return V_0_90_7;
            case V_0_90_6_ID:
                return V_0_90_6;
            case V_0_90_5_ID:
                return V_0_90_5;
            case V_0_90_4_ID:
                return V_0_90_4;
            case V_0_90_3_ID:
                return V_0_90_3;
            case V_0_90_2_ID:
                return V_0_90_2;
            case V_0_90_1_ID:
                return V_0_90_1;
            case V_0_90_0_ID:
                return V_0_90_0;
            case V_0_90_0_RC2_ID:
                return V_0_90_0_RC2;
            case V_0_90_0_RC1_ID:
                return V_0_90_0_RC1;
            case V_0_90_0_Beta1_ID:
                return V_0_90_0_Beta1;

            case V_0_20_7_ID:
                return V_0_20_7;
            case V_0_20_6_ID:
                return V_0_20_6;
            case V_0_20_5_ID:
                return V_0_20_5;
            case V_0_20_4_ID:
                return V_0_20_4;
            case V_0_20_3_ID:
                return V_0_20_3;
            case V_0_20_2_ID:
                return V_0_20_2;
            case V_0_20_1_ID:
                return V_0_20_1;
            case V_0_20_0_ID:
                return V_0_20_0;
            case V_0_20_0_RC1_ID:
                return V_0_20_0_RC1;

            case V_0_19_0_RC1_ID:
                return V_0_19_0_RC1;
            case V_0_19_0_RC2_ID:
                return V_0_19_0_RC2;
            case V_0_19_0_RC3_ID:
                return V_0_19_0_RC3;
            case V_0_19_0_ID:
                return V_0_19_0;
            case V_0_19_1_ID:
                return V_0_19_1;
            case V_0_19_2_ID:
                return V_0_19_2;
            case V_0_19_3_ID:
                return V_0_19_3;
            case V_0_19_4_ID:
                return V_0_19_4;
            case V_0_19_5_ID:
                return V_0_19_5;
            case V_0_19_6_ID:
                return V_0_19_6;
            case V_0_19_7_ID:
                return V_0_19_7;
            case V_0_19_8_ID:
                return V_0_19_8;
            case V_0_19_9_ID:
                return V_0_19_9;
            case V_0_19_10_ID:
                return V_0_19_10;
            case V_0_19_11_ID:
                return V_0_19_11;
            case V_0_19_12_ID:
                return V_0_19_12;
            case V_0_19_13_ID:
                return V_0_19_13;

            case V_0_18_0_ID:
                return V_0_18_0;
            case V_0_18_1_ID:
                return V_0_18_1;
            case V_0_18_2_ID:
                return V_0_18_2;
            case V_0_18_3_ID:
                return V_0_18_3;
            case V_0_18_4_ID:
                return V_0_18_4;
            case V_0_18_5_ID:
                return V_0_18_5;
            case V_0_18_6_ID:
                return V_0_18_6;
            case V_0_18_7_ID:
                return V_0_18_7;
            case V_0_18_8_ID:
                return V_0_18_8;

            default:
                return new Version(id, null, Lucene.VERSION);
        }
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

        String[] parts = version.split("\\.");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException("the version needs to contain major, minor and revision, and optionally the build");
        }

        try {
            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            int major = Integer.parseInt(parts[0]) * 1000000;
            int minor = Integer.parseInt(parts[1]) * 10000;
            int revision = Integer.parseInt(parts[2]) * 100;

            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("Beta")) {
                    build = Integer.parseInt(buildStr.substring(4));
                }
                if (buildStr.startsWith("RC")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                }
            }

            return fromId(major + minor + revision + build);

        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final Boolean snapshot;
    public final org.apache.lucene.util.Version luceneVersion;

    Version(int id, @Nullable Boolean snapshot, org.apache.lucene.util.Version luceneVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.snapshot = snapshot;
        this.luceneVersion = luceneVersion;
    }

    public boolean snapshot() {
        return snapshot != null && snapshot;
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
     * Just the version number (without -SNAPSHOT if snapshot).
     */
    public String number() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (build < 50) {
            sb.append(".Beta").append(build);
        } else if (build < 99) {
            sb.append(".RC").append(build - 50);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("Version: " + Version.CURRENT + ", Build: " + Build.CURRENT.hashShort() + "/" + Build.CURRENT.timestamp() + ", JVM: " + JvmInfo.jvmInfo().version());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(number());
        if (snapshot()) {
            sb.append("-SNAPSHOT");
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
