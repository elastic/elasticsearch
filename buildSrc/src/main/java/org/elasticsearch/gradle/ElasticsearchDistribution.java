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
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ElasticsearchDistribution implements Buildable {

    static final List<String> ALLOWED_PLATFORMS = Collections.unmodifiableList(Arrays.asList("linux", "windows", "darwin"));
    static final List<String> ALLOWED_TYPES =
        Collections.unmodifiableList(Arrays.asList("integ-test-zip", "archive", "rpm", "deb"));
    static final List<String> ALLOWED_FLAVORS = Collections.unmodifiableList(Arrays.asList("default", "oss"));

    // package private to tests can use
    static final String CURRENT_PLATFORM = OS.<String>conditional()
        .onLinux(() -> "linux")
        .onWindows(() -> "windows")
        .onMac(() -> "darwin")
        .supply();

    public static final class Extracted implements Buildable, Iterable<File> {

        // pkg private so plugin can configure
        final Configuration configuration;

        private Extracted(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public Iterator<File> iterator() {
            return configuration.iterator();
        }

        @Override
        public TaskDependency getBuildDependencies() {
            return configuration.getBuildDependencies();
        }

        @Override
        public String toString() {
            return configuration.getSingleFile().toString();
        }
    }

    private final String name;
    // pkg private so plugin can configure
    final Configuration configuration;
    private final Extracted extracted;

    private final Property<Version> version;
    private final Property<String> type;
    private final Property<String> platform;
    private final Property<String> flavor;
    private final Property<Boolean> bundledJdk;

    ElasticsearchDistribution(String name, Project project) {
        this.name = name;
        this.configuration = project.getConfigurations().create("es_distro_file_" + name);
        this.version = project.getObjects().property(Version.class);
        this.version.convention(Version.fromString(VersionProperties.getElasticsearch()));
        this.type = project.getObjects().property(String.class);
        this.type.convention("archive");
        this.platform = project.getObjects().property(String.class);
        this.flavor = project.getObjects().property(String.class);
        this.bundledJdk = project.getObjects().property(Boolean.class);
        this.extracted = new Extracted(project.getConfigurations().create("es_distro_extracted_" + name));
    }

    public String getName() {
        return name;
    }

    public Version getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        this.version.set(Version.fromString(version));
    }

    public String getPlatform() {
        return platform.get();
    }

    public void setPlatform(String platform) {
        if (ALLOWED_PLATFORMS.contains(platform) == false) {
            throw new IllegalArgumentException(
                "unknown platform [" + platform + "] for elasticsearch distribution [" + name + "], must be one of " + ALLOWED_PLATFORMS);
        }
        this.platform.set(platform);
    }

    public String getType() {
        return type.get();
    }

    public void setType(String type) {
        if (ALLOWED_TYPES.contains(type) == false) {
            throw new IllegalArgumentException(
                "unknown type [" + type+ "] for elasticsearch distribution [" + name + "], must be one of " + ALLOWED_TYPES);
        }
        this.type.set(type);
    }

    public String getFlavor() {
        return flavor.get();
    }

    public void setFlavor(String flavor) {
        if (ALLOWED_FLAVORS.contains(flavor) == false) {
            throw new IllegalArgumentException(
                "unknown flavor [" + flavor + "] for elasticsearch distribution [" + name + "], must be one of " + ALLOWED_FLAVORS);
        }
        this.flavor.set(flavor);
    }

    public boolean getBundledJdk() {
        return bundledJdk.get();
    }

    public void setBundledJdk(boolean bundledJdk) {
        this.bundledJdk.set(bundledJdk);
    }

    @Override
    public String toString() {
        return configuration.getSingleFile().toString();
    }

    public Extracted getExtracted() {
        if (getType().equals("rpm") || getType().equals("deb")) {
            throw new IllegalArgumentException("distribution type [" + getType() + "] for " +
                "elasticsearch distribution [" + name + "] cannot be extracted");
        }
        return extracted;
    }

    @Override
    public TaskDependency getBuildDependencies() {
        return configuration.getBuildDependencies();
    }

    // internal, make this distribution's configuration unmodifiable
    void finalizeValues() {

        if (getType().equals("integ-test-zip")) {
            if (platform.isPresent()) {
                throw new IllegalArgumentException(
                    "platform not allowed for elasticsearch distribution [" + name + "] of type [integ-test-zip]");
            }
            if (flavor.isPresent()) {
                throw new IllegalArgumentException(
                    "flavor not allowed for elasticsearch distribution [" + name + "] of type [integ-test-zip]");
            }
            if (bundledJdk.isPresent()) {
                throw new IllegalArgumentException(
                    "bundledJdk not allowed for elasticsearch distribution [" + name + "] of type [integ-test-zip]");
            }
            return;
        }

        if (getType().equals("archive")){
            // defaults for archive, set here instead of via convention so integ-test-zip can verify they are not set
            if (platform.isPresent() == false) {
                platform.set(CURRENT_PLATFORM);
            }
        } else { // rpm or deb
            if (platform.isPresent()) {
                throw new IllegalArgumentException("platform not allowed for elasticsearch distribution [" + name + "] of type [" + getType() + "]");
            }
        }

        if (flavor.isPresent() == false) {
            flavor.set("default");
        }
        if (bundledJdk.isPresent() == false) {
            bundledJdk.set(true);
        }

        version.finalizeValue();
        platform.finalizeValue();
        type.finalizeValue();
        flavor.finalizeValue();
        bundledJdk.finalizeValue();
    }


}
