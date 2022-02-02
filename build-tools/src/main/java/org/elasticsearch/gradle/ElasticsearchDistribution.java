/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.gradle.api.Action;
import org.gradle.api.Buildable;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Collections;
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

    // package private to tests can use
    public static final Platform CURRENT_PLATFORM = OS.<Platform>conditional()
        .onLinux(() -> Platform.LINUX)
        .onWindows(() -> Platform.WINDOWS)
        .onMac(() -> Platform.DARWIN)
        .supply();

    private final String name;
    private final Property<Boolean> dockerAvailability;
    // pkg private so plugin can configure
    final Configuration configuration;

    private final Property<Architecture> architecture;
    private final Property<String> version;
    private final Property<ElasticsearchDistributionType> type;
    private final Property<Platform> platform;
    private final Property<Boolean> bundledJdk;
    private final Property<Boolean> failIfUnavailable;
    private final Configuration extracted;
    private Action<ElasticsearchDistribution> distributionFinalizer;
    private boolean frozen = false;

    ElasticsearchDistribution(
        String name,
        ObjectFactory objectFactory,
        Property<Boolean> dockerAvailability,
        Configuration fileConfiguration,
        Configuration extractedConfiguration,
        Action<ElasticsearchDistribution> distributionFinalizer
    ) {
        this.name = name;
        this.dockerAvailability = dockerAvailability;
        this.configuration = fileConfiguration;
        this.architecture = objectFactory.property(Architecture.class);
        this.version = objectFactory.property(String.class).convention(VersionProperties.getElasticsearch());
        this.type = objectFactory.property(ElasticsearchDistributionType.class);
        this.type.convention(ElasticsearchDistributionTypes.ARCHIVE);
        this.platform = objectFactory.property(Platform.class);
        this.bundledJdk = objectFactory.property(Boolean.class);
        this.failIfUnavailable = objectFactory.property(Boolean.class).convention(true);
        this.extracted = extractedConfiguration;
        this.distributionFinalizer = distributionFinalizer;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        Version.fromString(version); // ensure the version parses, but don't store as Version since that removes -SNAPSHOT
        this.version.set(version);
    }

    public Platform getPlatform() {
        return platform.getOrNull();
    }

    public void setPlatform(Platform platform) {
        this.platform.set(platform);
    }

    public ElasticsearchDistributionType getType() {
        return type.get();
    }

    public void setType(ElasticsearchDistributionType type) {
        this.type.set(type);
    }

    /**
     * For simplicity only public distribution types are supported here
     * */
    public void setType(String type) {
        if (type.equals(ElasticsearchDistributionTypes.ARCHIVE.getName())) {
            this.type.set(ElasticsearchDistributionTypes.ARCHIVE);
        } else if (type.equals(ElasticsearchDistributionTypes.INTEG_TEST_ZIP.getName())) {
            this.type.set(ElasticsearchDistributionTypes.INTEG_TEST_ZIP);
        } else {
            throw new IllegalArgumentException("Cannot set Elasticsearch Distribution type to " + type + ". Type unknown.");
        }
    }

    public boolean getBundledJdk() {
        return bundledJdk.getOrElse(true);
    }

    public boolean isDocker() {
        return this.type.get().isDocker();
    }

    public void setBundledJdk(Boolean bundledJdk) {
        this.bundledJdk.set(bundledJdk);
    }

    public boolean getFailIfUnavailable() {
        return this.failIfUnavailable.get();
    }

    public void setFailIfUnavailable(boolean failIfUnavailable) {
        this.failIfUnavailable.set(failIfUnavailable);
    }

    public void setArchitecture(Architecture architecture) {
        this.architecture.set(architecture);
    }

    public Architecture getArchitecture() {
        return this.architecture.get();
    }

    @Override
    public String toString() {
        return getName() + "_" + getType() + "_" + getVersion();
    }

    /**
     * if not executed before, this
     * freezes the distribution configuration and
     * runs distribution finalizer logic.
     */
    public ElasticsearchDistribution maybeFreeze() {
        if (frozen == false) {
            finalizeValues();
            distributionFinalizer.execute(this);
            frozen = true;
        }
        return this;
    }

    public String getFilepath() {
        maybeFreeze();
        return configuration.getSingleFile().toString();
    }

    public Configuration getExtracted() {
        if (getType().shouldExtract() == false) {
            throw new UnsupportedOperationException(
                "distribution type [" + getType().getName() + "] for " + "elasticsearch distribution [" + name + "] cannot be extracted"
            );
        }
        return extracted;
    }

    @Override
    public TaskDependency getBuildDependencies() {
        if (skippingDockerDistributionBuild()) {
            return task -> Collections.emptySet();
        } else {
            maybeFreeze();
            return getType().shouldExtract() ? extracted.getBuildDependencies() : configuration.getBuildDependencies();
        }
    }

    private boolean skippingDockerDistributionBuild() {
        return isDocker() && getFailIfUnavailable() == false && dockerAvailability.get() == false;
    }

    @Override
    public Iterator<File> iterator() {
        maybeFreeze();
        return getType().shouldExtract() ? extracted.iterator() : configuration.iterator();
    }

    // internal, make this distribution's configuration unmodifiable
    void finalizeValues() {
        if (getType() == ElasticsearchDistributionTypes.INTEG_TEST_ZIP) {
            if (platform.getOrNull() != null) {
                throw new IllegalArgumentException(
                    "platform cannot be set on elasticsearch distribution [" + name + "] of type [integ_test_zip]"
                );
            }
            if (bundledJdk.getOrNull() != null) {
                throw new IllegalArgumentException(
                    "bundledJdk cannot be set on elasticsearch distribution [" + name + "] of type [integ_test_zip]"
                );
            }
            return;
        }

        if (isDocker() == false && failIfUnavailable.get() == false) {
            throw new IllegalArgumentException(
                "failIfUnavailable cannot be 'false' on elasticsearch distribution [" + name + "] of type [" + getType() + "]"
            );
        }

        if (getType() == ElasticsearchDistributionTypes.ARCHIVE) {
            // defaults for archive, set here instead of via convention so integ-test-zip can verify they are not set
            if (platform.isPresent() == false) {
                platform.set(CURRENT_PLATFORM);
            }
        } else { // rpm, deb or docker
            if (platform.isPresent()) {
                throw new IllegalArgumentException(
                    "platform cannot be set on elasticsearch distribution [" + name + "] of type [" + getType() + "]"
                );
            }
            if (isDocker()) {
                if (bundledJdk.isPresent()) {
                    throw new IllegalArgumentException(
                        "bundledJdk cannot be set on elasticsearch distribution [" + name + "] of type " + "[docker]"
                    );
                }
            }
        }

        if (bundledJdk.isPresent() == false) {
            bundledJdk.set(true);
        }

        version.finalizeValue();
        platform.finalizeValue();
        type.finalizeValue();
        bundledJdk.finalizeValue();
    }

    public TaskDependency getArchiveDependencies() {
        if (skippingDockerDistributionBuild()) {
            return task -> Collections.emptySet();
        } else {
            maybeFreeze();
            return configuration.getBuildDependencies();
        }
    }
}
