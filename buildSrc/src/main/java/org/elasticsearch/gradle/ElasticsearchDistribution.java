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
import java.util.Locale;

public class ElasticsearchDistribution implements Buildable, Iterable<File> {

    public enum Platform {
        LINUX,
        WINDOWS,
        DARWIN;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public enum Type {
        INTEG_TEST_ZIP,
        ARCHIVE,
        RPM,
        DEB;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public enum Flavor {
        DEFAULT,
        OSS;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    // package private to tests can use
    public static final Platform CURRENT_PLATFORM = OS.<Platform>conditional()
        .onLinux(() -> Platform.LINUX)
        .onWindows(() -> Platform.WINDOWS)
        .onMac(() -> Platform.DARWIN)
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
    private final Property<Type> type;
    private final Property<Platform> platform;
    private final Property<Flavor> flavor;
    private final Property<Boolean> bundledJdk;

    ElasticsearchDistribution(String name, ObjectFactory objectFactory, Configuration fileConfiguration,
                              Configuration extractedConfiguration) {
        this.name = name;
        this.configuration = fileConfiguration;
        this.version = objectFactory.property(Version.class);
        this.version.convention(Version.fromString(VersionProperties.getElasticsearch()));
        this.type = objectFactory.property(Type.class);
        this.type.convention(Type.ARCHIVE);
        this.platform = objectFactory.property(Platform.class);
        this.flavor = objectFactory.property(Flavor.class);
        this.bundledJdk = objectFactory.property(Boolean.class);
        this.extracted = new Extracted(extractedConfiguration);
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

    public Platform getPlatform() {
        return platform.getOrNull();
    }

    public void setPlatform(Platform platform) {
        this.platform.set(platform);
    }

    public Type getType() {
        return type.get();
    }

    public void setType(Type type) {
        this.type.set(type);
    }

    public Flavor getFlavor() {
        return flavor.getOrNull();
    }

    public void setFlavor(Flavor flavor) {
        this.flavor.set(flavor);
    }

    public boolean getBundledJdk() {
        return bundledJdk.getOrElse(true);
    }

    public void setBundledJdk(boolean bundledJdk) {
        this.bundledJdk.set(bundledJdk);
    }

    @Override
    public String toString() {
        return configuration.getSingleFile().toString();
    }

    public Extracted getExtracted() {
        if (getType() == Type.RPM || getType() == Type.DEB) {
            throw new UnsupportedOperationException("distribution type [" + getType() + "] for " +
                "elasticsearch distribution [" + name + "] cannot be extracted");
        }
        return extracted;
    }

    @Override
    public TaskDependency getBuildDependencies() {
        return configuration.getBuildDependencies();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }

    // TODO: remove this when distro tests are per distribution
    public Configuration getConfiguration() {
        return configuration;
    }

    // internal, make this distribution's configuration unmodifiable
    void finalizeValues() {

        if (getType() == Type.INTEG_TEST_ZIP) {
            if (platform.isPresent()) {
                throw new IllegalArgumentException(
                    "platform not allowed for elasticsearch distribution [" + name + "] of type [integ_test_zip]");
            }
            if (flavor.isPresent()) {
                throw new IllegalArgumentException(
                    "flavor not allowed for elasticsearch distribution [" + name + "] of type [integ_test_zip]");
            }
            if (bundledJdk.isPresent()) {
                throw new IllegalArgumentException(
                    "bundledJdk not allowed for elasticsearch distribution [" + name + "] of type [integ_test_zip]");
            }
            return;
        }

        if (getType() == Type.ARCHIVE) {
            // defaults for archive, set here instead of via convention so integ-test-zip can verify they are not set
            if (platform.isPresent() == false) {
                platform.set(CURRENT_PLATFORM);
            }
        } else { // rpm or deb
            if (platform.isPresent()) {
                throw new IllegalArgumentException("platform not allowed for elasticsearch distribution ["
                    + name + "] of type [" + getType() + "]");
            }
        }

        if (flavor.isPresent() == false) {
            flavor.set(Flavor.DEFAULT);
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
