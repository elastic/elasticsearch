/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.transform.FilteringJarTransform;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.Usage;
import org.gradle.api.tasks.Copy;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DistributionLayoutPluginTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void createsExpectedConfigurationsAndAttributes() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("project")).build();

        project.getPlugins().apply(DistributionLayoutPlugin.class);
        DistributionLayoutExtension extension = extension(project);
        registerLibraries(project, extension);

        for (String name : List.of(
            "libs",
            "libsVersionChecker",
            "libsCliLauncher",
            "libsServerCli",
            "libsServerLauncher",
            "libsServerLauncherNativeX64",
            "libsServerLauncherNativeAarch64",
            "libsWindowsServiceCli",
            "libsPluginCli",
            "libsKeystoreCli",
            "libsSecurityCli",
            "libsGeoIpCli",
            "libsAnsiConsole",
            "libsNative",
            "libsEntitlementAgent",
            "libsEntitlementBridge"
        )) {
            Configuration configuration = project.getConfigurations().getByName(name);
            assertEquals(false, configuration.isCanBeConsumed());
            assertEquals(true, configuration.isCanBeResolved());
            assertEquals(Category.LIBRARY, configuration.getAttributes().getAttribute(Category.CATEGORY_ATTRIBUTE).getName());
            assertEquals(Usage.JAVA_RUNTIME, configuration.getAttributes().getAttribute(Usage.USAGE_ATTRIBUTE).getName());
            assertEquals(Bundling.EXTERNAL, configuration.getAttributes().getAttribute(Bundling.BUNDLING_ATTRIBUTE).getName());
        }

        assertEquals(
            FilteringJarTransform.FILTERED_JAR_TYPE,
            project.getConfigurations()
                .getByName("libsEntitlementBridge")
                .getAttributes()
                .getAttribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE)
        );
    }

    @Test
    public void duplicateConfigurationCreationFailsLoudly() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("duplicate")).build();
        project.getPlugins().apply(DistributionLayoutPlugin.class);

        DistributionLayoutExtension extension = extension(project);
        extension.library("libs", library -> {
            library.from(project.files("server.jar"));
            library.into("");
        });

        InvalidUserDataException error = assertThrows(InvalidUserDataException.class, () -> extension.library("libs", library -> {
            library.from(project.files("server-duplicate.jar"));
            library.into("");
        }));

        assertTrue(error.getMessage().contains("libs"));
    }

    @Test
    public void registersEntitlementBridgeTransform() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("transform")).build();
        project.getPlugins().apply(DistributionLayoutPlugin.class);

        File inputJar = temporaryFolder.newFile("bridge.jar");
        writeJar(inputJar);

        DistributionLayoutExtension extension = extension(project);
        extension.library("libsEntitlementBridge", library -> {
            library.from(project.files(inputJar));
            library.into("entitlement-bridge");
            library.filtered();
        });

        File transformed = project.getConfigurations().getByName("libsEntitlementBridge").getSingleFile();

        assertNotEquals(inputJar.getCanonicalFile(), transformed.getCanonicalFile());
        try (ZipFile zip = new ZipFile(transformed)) {
            Set<String> entries = new LinkedHashSet<>();
            zip.stream().forEach(entry -> entries.add(entry.getName()));
            assertTrue(entries.contains("keep.txt"));
            assertEquals(false, entries.contains("module-info.class"));
            assertEquals(false, entries.contains("META-INF/versions/9/bridge.class"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void copyTaskInputsContainRequestedTokenExpansions() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("inputs")).build();
        writeLicenses(project.getProjectDir().toPath());
        project.getPlugins().apply(DistributionLayoutPlugin.class);

        DistributionLayoutExtension extension = extension(project);
        extension.tokenExpansions("zip", true);
        extension.tokenExpansions("deb", false);

        Copy copy = project.getTasks().create("copyLayout", Copy.class);
        Map<String, Map<String, String>> inputValue = (Map<String, Map<String, String>>) copy.getInputs()
            .getProperties()
            .get("esDistributionLayout.tokenExpansions");

        assertEquals(List.of("deb:false", "zip:true"), new ArrayList<>(inputValue.keySet()));
        assertEquals("deb", inputValue.get("deb:false").get("es.distribution.type"));
        assertEquals("zip", inputValue.get("zip:true").get("es.distribution.type"));
        inputValue.values().forEach(expansions -> {
            List<String> keys = new ArrayList<>(expansions.keySet());
            List<String> sortedKeys = new ArrayList<>(keys);
            Collections.sort(sortedKeys);
            assertEquals(sortedKeys, keys);
        });
    }

    private static DistributionLayoutExtension extension(Project project) {
        return (DistributionLayoutExtension) project.getExtensions().getByName("esDistributionLayout");
    }

    private static void registerLibraries(Project project, DistributionLayoutExtension extension) {
        extension.library("libs", library -> {
            library.from(project.files("server.jar"));
            library.into("");
        });
        extension.library("libsVersionChecker", library -> {
            library.from(project.files("java-version-checker.jar"));
            library.into("java-version-checker");
        });
        extension.library("libsCliLauncher", library -> {
            library.from(project.files("cli-launcher.jar"));
            library.into("cli-launcher");
        });
        extension.library("libsServerCli", library -> {
            library.from(project.files("server-cli.jar"));
            library.into("tools/server-cli");
        });
        extension.library("libsServerLauncher", library -> {
            library.from(project.files("server-launcher.jar"));
            library.into("tools/server-launcher");
            library.serverLauncher();
            library.nativeX64(project.files("server-launcher-linux-x64"));
            library.nativeAarch64(project.files("server-launcher-linux-aarch64"));
        });
        extension.library("libsWindowsServiceCli", library -> {
            library.from(project.files("windows-service-cli.jar"));
            library.into("tools/windows-service-cli");
        });
        extension.library("libsGeoIpCli", library -> {
            library.from(project.files("geoip-cli.jar"));
            library.into("tools/geoip-cli");
        });
        extension.library("libsPluginCli", library -> {
            library.from(project.files("plugin-cli.jar"));
            library.into("tools/plugin-cli");
        });
        extension.library("libsKeystoreCli", library -> {
            library.from(project.files("keystore-cli.jar"));
            library.into("tools/keystore-cli");
        });
        extension.library("libsSecurityCli", library -> {
            library.from(project.files("security-cli.jar"));
            library.into("tools/security-cli");
        });
        extension.library("libsAnsiConsole", library -> {
            library.from(project.files("ansi-console.jar"));
            library.into("tools/ansi-console");
        });
        extension.library("libsNative", library -> {
            library.from(project.files("platform"));
            library.into("platform");
            library.platformNative();
        });
        extension.library("libsEntitlementAgent", library -> {
            library.from(project.files("entitlement-agent.jar"));
            library.into("entitlement-agent");
        });
        extension.library("libsEntitlementBridge", library -> {
            library.from(project.files("entitlement-bridge.jar"));
            library.into("entitlement-bridge");
            library.filtered();
        });
    }

    private static void writeJar(File file) throws IOException {
        try (JarOutputStream output = new JarOutputStream(new FileOutputStream(file))) {
            output.putNextEntry(new JarEntry("keep.txt"));
            output.write("ok".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();

            output.putNextEntry(new JarEntry("module-info.class"));
            output.write(new byte[] { 0x1, 0x2 });
            output.closeEntry();

            output.putNextEntry(new JarEntry("META-INF/versions/9/bridge.class"));
            output.write(new byte[] { 0x3, 0x4 });
            output.closeEntry();
        }
    }

    private static void writeLicenses(java.nio.file.Path projectDir) throws IOException {
        java.nio.file.Path licenses = Files.createDirectories(projectDir.resolve("licenses"));
        Files.writeString(licenses.resolve(TokenExpansions.DEFAULT_LICENSE_FILE), "default license", StandardCharsets.UTF_8);
        Files.writeString(licenses.resolve(TokenExpansions.TEST_DISTRIBUTION_LICENSE_FILE), "test license", StandardCharsets.UTF_8);
    }
}
