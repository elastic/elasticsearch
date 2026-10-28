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
import org.gradle.api.internal.plugins.PluginApplicationException;
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

        for (String name : List.of(
            DistributionLibraries.LIBS,
            DistributionLibraries.LIBS_VERSION_CHECKER,
            DistributionLibraries.LIBS_CLI_LAUNCHER,
            DistributionLibraries.LIBS_SERVER_CLI,
            DistributionLibraries.LIBS_SERVER_LAUNCHER,
            DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_X64,
            DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_AARCH64,
            DistributionLibraries.LIBS_WINDOWS_SERVICE_CLI,
            DistributionLibraries.LIBS_PLUGIN_CLI,
            DistributionLibraries.LIBS_KEYSTORE_CLI,
            DistributionLibraries.LIBS_SECURITY_CLI,
            DistributionLibraries.LIBS_GEOIP_CLI,
            DistributionLibraries.LIBS_ANSI_CONSOLE,
            DistributionLibraries.LIBS_NATIVE,
            DistributionLibraries.LIBS_ENTITLEMENT_AGENT,
            DistributionLibraries.LIBS_ENTITLEMENT_BRIDGE
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
                .getByName(DistributionLibraries.LIBS_ENTITLEMENT_BRIDGE)
                .getAttributes()
                .getAttribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE)
        );
    }

    @Test
    public void duplicateConfigurationCreationFailsLoudly() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("duplicate")).build();
        project.getConfigurations().create(DistributionLibraries.LIBS);

        PluginApplicationException error = assertThrows(PluginApplicationException.class, () -> project.getPlugins().apply(DistributionLayoutPlugin.class));

        assertTrue(error.getCause() instanceof InvalidUserDataException);
        assertTrue(error.getCause().getMessage().contains(DistributionLibraries.LIBS));
    }

    @Test
    public void registersEntitlementBridgeTransform() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("transform")).build();
        project.getPlugins().apply(DistributionLayoutPlugin.class);

        File inputJar = temporaryFolder.newFile("bridge.jar");
        writeJar(inputJar);

        project.getDependencies().add(DistributionLibraries.LIBS_ENTITLEMENT_BRIDGE, project.files(inputJar));

        File transformed = project.getConfigurations().getByName(DistributionLibraries.LIBS_ENTITLEMENT_BRIDGE).getSingleFile();

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

        DistributionLayoutExtension extension = (DistributionLayoutExtension) project.getExtensions().getByName("esDistributionLayout");
        extension.tokenExpansions("deb", false);
        extension.tokenExpansions("zip", true);

        Copy copy = project.getTasks().create("copyLayout", Copy.class);
        Map<String, Map<String, String>> inputValue = (Map<String, Map<String, String>>) copy.getInputs()
            .getProperties()
            .get("esDistributionLayout.tokenExpansions");

        assertEquals(Set.of("deb:false", "zip:true"), inputValue.keySet());
        assertEquals("deb", inputValue.get("deb:false").get("es.distribution.type"));
        assertEquals("zip", inputValue.get("zip:true").get("es.distribution.type"));
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
