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

package org.elasticsearch.gradle;

import org.gradle.api.Buildable;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Jdk implements Buildable, Iterable<File> {

    private static final List<String> ALLOWED_VENDORS = List.of("adoptopenjdk", "openjdk");
    private static final List<String> ALLOWED_PLATFORMS = List.of("darwin", "linux", "windows", "mac");
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(\\.\\d+\\.\\d+)?\\+(\\d+(?:\\.\\d+)?)(@([a-f0-9]{32}))?");
    private static final Pattern LEGACY_VERSION_PATTERN = Pattern.compile("(\\d)(u\\d+)\\+(b\\d+?)(@([a-f0-9]{32}))?");

    private final String name;
    private final Configuration configuration;

    private final Property<String> vendor;
    private final Property<String> version;
    private final Property<String> platform;
    private String baseVersion;
    private String major;
    private String build;
    private String hash;

    Jdk(String name, Configuration configuration, ObjectFactory objectFactory) {
        this.name = name;
        this.configuration = configuration;
        this.vendor = objectFactory.property(String.class);
        this.version = objectFactory.property(String.class);
        this.platform = objectFactory.property(String.class);
    }

    public String getName() {
        return name;
    }

    public String getVendor() {
        return vendor.get();
    }

    public void setVendor(final String vendor) {
        if (ALLOWED_VENDORS.contains(vendor) == false) {
            throw new IllegalArgumentException("unknown vendor [" + vendor + "] for jdk [" + name + "], must be one of " + ALLOWED_VENDORS);
        }
        this.vendor.set(vendor);
    }

    public String getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        if (VERSION_PATTERN.matcher(version).matches() == false && LEGACY_VERSION_PATTERN.matcher(version).matches() == false) {
            throw new IllegalArgumentException("malformed version [" + version + "] for jdk [" + name + "]");
        }
        parseVersion(version);
        this.version.set(version);
    }

    public String getPlatform() {
        return platform.get();
    }

    public void setPlatform(String platform) {
        if (ALLOWED_PLATFORMS.contains(platform) == false) {
            throw new IllegalArgumentException(
                "unknown platform [" + platform + "] for jdk [" + name + "], must be one of " + ALLOWED_PLATFORMS
            );
        }
        this.platform.set(platform);
    }

    public String getBaseVersion() {
        return baseVersion;
    }

    public String getMajor() {
        return major;
    }

    public String getBuild() {
        return build;
    }

    public String getHash() {
        return hash;
    }

    public String getPath() {
        return configuration.getSingleFile().toString();
    }

    public String getConfigurationName() {
        return configuration.getName();
    }

    @Override
    public String toString() {
        return getPath();
    }

    @Override
    public TaskDependency getBuildDependencies() {
        return configuration.getBuildDependencies();
    }

    public Object getBinJavaPath() {
        return new Object() {
            @Override
            public String toString() {
                final String platform = getPlatform();
                final boolean isOSX = "mac".equals(platform) || "darwin".equals(platform);
                return getPath() + (isOSX ? "/Contents/Home" : "") + "/bin/java";
            }
        };
    }

    // internal, make this jdks configuration unmodifiable
    void finalizeValues() {
        if (version.isPresent() == false) {
            throw new IllegalArgumentException("version not specified for jdk [" + name + "]");
        }
        if (platform.isPresent() == false) {
            throw new IllegalArgumentException("platform not specified for jdk [" + name + "]");
        }
        if (vendor.isPresent() == false) {
            throw new IllegalArgumentException("vendor not specified for jdk [" + name + "]");
        }
        version.finalizeValue();
        platform.finalizeValue();
        vendor.finalizeValue();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }

    private void parseVersion(String version) {
        // decompose the bundled jdk version, broken into elements as: [feature, interim, update, build]
        // Note the "patch" version is not yet handled here, as it has not yet been used by java.
        Matcher jdkVersionMatcher = VERSION_PATTERN.matcher(version);
        if (jdkVersionMatcher.matches() == false) {
            // Try again with the pre-Java9 version format
            jdkVersionMatcher = LEGACY_VERSION_PATTERN.matcher(version);

            if (jdkVersionMatcher.matches() == false) {
                throw new IllegalArgumentException("Malformed jdk version [" + version + "]");
            }
        }

        baseVersion = jdkVersionMatcher.group(1) + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        major = jdkVersionMatcher.group(1);
        build = jdkVersionMatcher.group(3);
        hash = jdkVersionMatcher.group(5);
    }

}
