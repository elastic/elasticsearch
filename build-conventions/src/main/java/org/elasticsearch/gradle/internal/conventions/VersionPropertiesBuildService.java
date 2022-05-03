/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.apache.commons.io.FileUtils;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.inject.Inject;

abstract class VersionPropertiesBuildService implements BuildService<VersionPropertiesBuildService.Params>, AutoCloseable {

    private final Properties properties;

    @Inject
    public VersionPropertiesBuildService(ProviderFactory providerFactory) {
        File infoPath = getParameters().getInfoPath().getAsFile().get();
        try {
            File propertiesInputFile = new File(infoPath, "version.properties");
            properties = VersionPropertiesLoader.loadBuildSrcVersion(propertiesInputFile, providerFactory);
            properties.computeIfAbsent("minimumRuntimeJava", s -> resolveMinimumRuntimeJavaVersion(infoPath));
            properties.computeIfAbsent("minimumCompilerJava", s -> resolveMinimumCompilerJavaVersion(infoPath));
        } catch (IOException e) {
            throw new GradleException("Cannot load VersionPropertiesBuildService", e);
        }
    }

    private JavaVersion resolveMinimumRuntimeJavaVersion(File infoPath) {
        return resolveJavaVersion(infoPath, "src/main/resources/minimumRuntimeVersion");
    }

    private JavaVersion resolveMinimumCompilerJavaVersion(File infoPath) {
        return resolveJavaVersion(infoPath, "src/main/resources/minimumCompilerVersion");
    }

    private JavaVersion resolveJavaVersion(File infoPath, String path) {
        final JavaVersion minimumJavaVersion;
        File minimumJavaInfoSource = new File(infoPath, path);
        try {
            String versionString = FileUtils.readFileToString(minimumJavaInfoSource);
            minimumJavaVersion = JavaVersion.toVersion(versionString);
        } catch (IOException e) {
            throw new GradleException("Cannot resolve minimum compiler version via VersionPropertiesBuildService", e);
        }
        return minimumJavaVersion;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public void close() throws Exception {
    }

    public interface Params extends BuildServiceParameters {
        RegularFileProperty getInfoPath();
    }

}
