/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.Version;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class TokenExpansionsTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Version VERSION = Version.fromString("9.3.1");

    private static Map<String, String> expansions(String type) {
        return TokenExpansions.forDistribution(DistributionType.fromId(type), "packages", "9.3.1-SNAPSHOT", VERSION, "line one\nline two");
    }

    @Test
    public void commonExpansions() {
        for (String type : List.of("tar", "zip", "deb", "rpm")) {
            Map<String, String> expansions = expansions(type);
            assertEquals("packages", expansions.get("project.name"));
            assertEquals("9.3.1-SNAPSHOT", expansions.get("project.version"));
            assertEquals("9.3", expansions.get("project.minor.version"));
            assertEquals(type, expansions.get("es.distribution.type"));
        }
    }

    @Test
    public void archiveExpansions() {
        for (String type : List.of("tar", "zip")) {
            Map<String, String> expansions = expansions(type);
            assertEquals("\"$ES_HOME\"/config", expansions.get("path.conf"));
            assertEquals("#path.data: /path/to/data", expansions.get("path.data"));
            assertEquals("#path.logs: /path/to/logs", expansions.get("path.logs"));
            assertEquals("if [ -z \"$ES_PATH_CONF\" ]; then ES_PATH_CONF=\"$ES_HOME\"/config; fi", expansions.get("source.path.env"));
            assertEquals("# Built for packages-9.3.1-SNAPSHOT (" + type + ")", expansions.get("scripts.footer"));
            assertFalse(expansions.containsKey("path.env"));
            assertFalse(expansions.containsKey("license.name"));
            assertFalse(expansions.containsKey("license.text"));
        }
    }

    @Test
    public void debExpansions() {
        Map<String, String> expansions = expansions("deb");
        assertEquals("/etc/elasticsearch", expansions.get("path.conf"));
        assertEquals("path.data: /var/lib/elasticsearch", expansions.get("path.data"));
        assertEquals("path.logs: /var/log/elasticsearch", expansions.get("path.logs"));
        assertEquals("/etc/default/elasticsearch", expansions.get("path.env"));
        assertEquals("source /etc/default/elasticsearch", expansions.get("source.path.env"));
        assertEquals("exit 0\n# Built for packages-9.3.1-SNAPSHOT (deb)", expansions.get("scripts.footer"));
        assertEquals("Elastic-License", expansions.get("license.name"));
        assertEquals(" line one\n line two", expansions.get("license.text"));
    }

    @Test
    public void rpmExpansions() {
        Map<String, String> expansions = expansions("rpm");
        assertEquals("/etc/elasticsearch", expansions.get("path.conf"));
        assertEquals("path.data: /var/lib/elasticsearch", expansions.get("path.data"));
        assertEquals("path.logs: /var/log/elasticsearch", expansions.get("path.logs"));
        assertEquals("/etc/sysconfig/elasticsearch", expansions.get("path.env"));
        assertEquals("source /etc/sysconfig/elasticsearch", expansions.get("source.path.env"));
        assertEquals("# Built for packages-9.3.1-SNAPSHOT (rpm)", expansions.get("scripts.footer"));
        assertFalse(expansions.containsKey("license.name"));
        assertFalse(expansions.containsKey("license.text"));
    }

    @Test
    public void expansionKeysAreStable() {
        assertEquals(
            List.of(
                "project.name",
                "project.version",
                "project.minor.version",
                "path.conf",
                "path.data",
                "path.env",
                "source.path.env",
                "path.logs",
                "scripts.footer",
                "es.distribution.type",
                "license.name",
                "license.text"
            ),
            List.copyOf(expansions("deb").keySet())
        );
    }

    @Test
    public void unknownDistributionTypeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> DistributionType.fromId("docker"));
    }

    @Test
    public void forProjectReadsLicenseOfDistributionFlavour() throws IOException {
        File projectDir = temporaryFolder.newFolder("project");
        Path licenses = Files.createDirectories(projectDir.toPath().resolve("licenses"));
        Files.writeString(licenses.resolve(TokenExpansions.DEFAULT_LICENSE_FILE), "default", StandardCharsets.UTF_8);
        Files.writeString(licenses.resolve(TokenExpansions.TEST_DISTRIBUTION_LICENSE_FILE), "test", StandardCharsets.UTF_8);
        Project project = ProjectBuilder.builder().withName("packages").withProjectDir(projectDir).build();

        assertEquals(" default", TokenExpansions.forProject(project, "deb", false).get("license.text"));
        assertEquals(" test", TokenExpansions.forProject(project, "deb", true).get("license.text"));
        assertEquals("packages", TokenExpansions.forProject(project, "rpm", false).get("project.name"));
    }

    @Test
    public void forProjectFailsOnMissingLicenseFile() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder("empty")).build();
        assertThrows(IllegalStateException.class, () -> TokenExpansions.forProject(project, "deb", false));
    }

    @Test
    public void licenseFileName() {
        assertEquals("ELASTIC-LICENSE-2.0.txt", TokenExpansions.licenseFileName(false));
        assertEquals("AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt", TokenExpansions.licenseFileName(true));
    }
}
