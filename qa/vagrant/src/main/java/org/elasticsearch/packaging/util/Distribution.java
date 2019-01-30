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

package org.elasticsearch.packaging.util;

import java.util.Locale;

public enum Distribution {

    OSS_LINUX(Packaging.TAR, Platform.LINUX, Flavor.OSS),
    OSS_WINDOWS(Packaging.ZIP, Platform.WINDOWS, Flavor.OSS),
    OSS_DARWIN(Packaging.TAR, Platform.DARWIN, Flavor.OSS),
    OSS_DEB(Packaging.DEB, Platform.LINUX, Flavor.OSS),
    OSS_RPM(Packaging.RPM, Platform.LINUX, Flavor.OSS),

    DEFAULT_LINUX(Packaging.TAR, Platform.LINUX, Flavor.DEFAULT),
    DEFAULT_WINDOWS(Packaging.ZIP, Platform.WINDOWS, Flavor.DEFAULT),
    DEFAULT_DARWIN(Packaging.TAR, Platform.DARWIN, Flavor.DEFAULT),
    DEFAULT_DEB(Packaging.DEB, Platform.LINUX, Flavor.DEFAULT),
    DEFAULT_RPM(Packaging.RPM, Platform.LINUX, Flavor.DEFAULT);

    public final Packaging packaging;
    public final Platform platform;
    public final Flavor flavor;

    Distribution(Packaging packaging, Platform platform, Flavor flavor) {
        this.packaging = packaging;
        this.platform = platform;
        this.flavor = flavor;
    }

    public String filename(String version) {
        String architecture = "";
        if (version.startsWith("6.") == false) {

            if (packaging == Packaging.DEB) {
                architecture = "-amd64";
            } else {
                if (packaging != Packaging.RPM) {
                    architecture = "-" + platform.toString();
                }
                architecture += "-x86_64";
            }
        }
        return flavor.name + "-" + version + architecture + packaging.extension;
    }

    public boolean isDefault() {
        return flavor.equals(Flavor.DEFAULT);
    }

    public boolean isOSS() {
        return flavor.equals(Flavor.OSS);
    }

    public enum Packaging {

        TAR(".tar.gz", Platforms.LINUX || Platforms.DARWIN),
        ZIP(".zip", Platforms.WINDOWS),
        DEB(".deb", Platforms.isDPKG()),
        RPM(".rpm", Platforms.isRPM());

        /** The extension of this distribution's file */
        public final String extension;

        /** Whether the distribution is intended for use on the platform the current JVM is running on */
        public final boolean compatible;

        Packaging(String extension, boolean compatible) {
            this.extension = extension;
            this.compatible = compatible;
        }
    }

    public enum Platform {
        LINUX,
        WINDOWS,
        DARWIN;

        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum Flavor {

        OSS("elasticsearch-oss"),
        DEFAULT("elasticsearch");

        public final String name;

        Flavor(String name) {
            this.name = name;
        }
    }
}
