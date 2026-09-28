/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.jvm.toolchain.JvmVendorSpec;

import javax.inject.Inject;

import static org.elasticsearch.gradle.util.GradleUtils.withRetries;

public abstract class ResolveAllDependencies extends DefaultTask {

    @Inject
    protected abstract JavaToolchainService getJavaToolchainService();

    @InputFiles
    public abstract ConfigurableFileCollection getResolvedArtifacts();

    @Input
    public abstract Property<Boolean> getResolveJavaToolChain();

    @Inject
    public ResolveAllDependencies() {
        getResolveJavaToolChain().convention(false);
    }

    @TaskAction
    void resolveAll() {
        if (getResolveJavaToolChain().get()) {
            withRetries(() -> {
                resolveDefaultJavaToolChain();
                resolveJdk17FallbackJavaToolChain();
            });
        }
    }

    private void resolveJdk17FallbackJavaToolChain() {
        getJavaToolchainService().launcherFor(RestTestBasePlugin.JAVA_TOOLCHAIN_JDK_ADOPTIUM_SPEC_ACTION).get();
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
}
