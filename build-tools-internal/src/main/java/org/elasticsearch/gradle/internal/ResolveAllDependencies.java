/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.jvm.toolchain.JvmVendorSpec;

import java.util.Collection;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static org.elasticsearch.gradle.DistributionDownloadPlugin.DISTRO_EXTRACTED_CONFIG_PREFIX;
import static org.elasticsearch.gradle.internal.test.rest.compat.compat.LegacyYamlRestCompatTestPlugin.BWC_MINOR_CONFIG_NAME;

public abstract class ResolveAllDependencies extends DefaultTask {

    private boolean resolveJavaToolChain = false;

    @Inject
    protected abstract JavaToolchainService getJavaToolchainService();

    private final ObjectFactory objectFactory;

    private Collection<Configuration> configs;

    @Inject
    public ResolveAllDependencies(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    @InputFiles
    public FileCollection getResolvedArtifacts() {
        return objectFactory.fileCollection()
            .from(configs.stream().filter(ResolveAllDependencies::canBeResolved).collect(Collectors.toList()));
    }

    @TaskAction
    void resolveAll() {
        if (resolveJavaToolChain) {
            resolveDefaultJavaToolChain();
        }
    }

    private void resolveDefaultJavaToolChain() {
        getJavaToolchainService().launcherFor(javaToolchainSpec -> {
            String bundledVendor = VersionProperties.getBundledJdkVendor();
            String bundledJdkMajorVersion = VersionProperties.getBundledJdkMajorVersion();
            javaToolchainSpec.getLanguageVersion().set(JavaLanguageVersion.of(bundledJdkMajorVersion));
            javaToolchainSpec.getVendor()
                .set(bundledVendor.equals("openjdk") ? JvmVendorSpec.ORACLE : JvmVendorSpec.matching(bundledVendor));
        }).get();
    }

    @Internal
    public Collection<Configuration> getConfigs() {
        return configs;
    }

    public void setConfigs(Collection<Configuration> configs) {
        this.configs = configs;
    }

    @Internal
    public boolean isResolveJavaToolChain() {
        return resolveJavaToolChain;
    }

    public void setResolveJavaToolChain(boolean resolveJavaToolChain) {
        this.resolveJavaToolChain = resolveJavaToolChain;
    }

    private static boolean canBeResolved(Configuration configuration) {
        if (configuration.isCanBeResolved() == false) {
            return false;
        }
        if (configuration instanceof org.gradle.internal.deprecation.DeprecatableConfiguration deprecatableConfiguration) {
            if (deprecatableConfiguration.canSafelyBeResolved() == false) {
                return false;
            }
        }
        return configuration.getName().startsWith(DISTRO_EXTRACTED_CONFIG_PREFIX) == false
            && configuration.getName().equals(BWC_MINOR_CONFIG_NAME) == false;
    }

}
