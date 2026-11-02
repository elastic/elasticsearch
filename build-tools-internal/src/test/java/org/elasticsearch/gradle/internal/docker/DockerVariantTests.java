/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.internal.DockerBase;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DockerVariantTests {

    private static final String VERSION = "9.3.1-SNAPSHOT";

    @Test
    public void allCoversEveryArchitectureAndBase() {
        List<DockerVariant> all = DockerVariant.all();
        assertEquals(Architecture.values().length * DockerBase.values().length, all.size());
        assertEquals(new DockerVariant(Architecture.X64, DockerBase.DEFAULT), all.get(0));
        assertEquals(new DockerVariant(Architecture.AARCH64, DockerBase.CLOUD_ESS_FIPS), all.get(all.size() - 1));
    }

    @Test
    public void camelNames() {
        assertEquals("buildDockerImage", new DockerVariant(Architecture.X64, DockerBase.DEFAULT).camelName("build", "DockerImage"));
        assertEquals(
            "buildAarch64DockerContext",
            new DockerVariant(Architecture.AARCH64, DockerBase.DEFAULT).camelName("build", "DockerContext")
        );
        assertEquals(
            "transformIronBankDockerContext",
            new DockerVariant(Architecture.X64, DockerBase.IRON_BANK).camelName("transform", "DockerContext")
        );
        assertEquals(
            "buildAarch64CloudEssFipsDockerImage",
            new DockerVariant(Architecture.AARCH64, DockerBase.CLOUD_ESS_FIPS).camelName("build", "DockerImage")
        );
        assertEquals("dockerWolfiContext", new DockerVariant(Architecture.X64, DockerBase.WOLFI).camelName("docker", "Context"));
    }

    @Test
    public void imageNames() {
        DockerVariant variant = new DockerVariant(Architecture.AARCH64, DockerBase.WOLFI);
        assertEquals("elasticsearch-wolfi", variant.imageName());
        assertEquals("elasticsearch-wolfi:aarch64", variant.localImageReference());
        assertEquals("elasticsearch-wolfi-" + VERSION + "-docker-build-context-aarch64", variant.buildContextName(VERSION));
        assertEquals(
            "elasticsearch-" + VERSION + "-docker-build-context",
            new DockerVariant(Architecture.X64, DockerBase.DEFAULT).buildContextName(VERSION)
        );
    }

    @Test
    public void tagsForOtherArchitecture() {
        DockerVariant variant = new DockerVariant(Architecture.AARCH64, DockerBase.DEFAULT);
        assertEquals(List.of("elasticsearch:aarch64"), variant.tags(VERSION, Architecture.X64));
    }

    @Test
    public void tagsForCurrentArchitecture() {
        DockerVariant variant = new DockerVariant(Architecture.X64, DockerBase.WOLFI);
        assertEquals(
            List.of(
                "elasticsearch-wolfi:x86_64",
                "elasticsearch-wolfi:test",
                "elasticsearch-wolfi:" + VERSION,
                "docker.elastic.co/elasticsearch/elasticsearch-wolfi:" + VERSION
            ),
            variant.tags(VERSION, Architecture.X64)
        );
    }

    @Test
    public void cloudEssIsTaggedInCiNamespace() {
        DockerVariant variant = new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS);
        assertEquals(
            "docker.elastic.co/elasticsearch-ci/elasticsearch-cloud-ess:" + VERSION,
            variant.tags(VERSION, Architecture.X64).get(3)
        );
    }

    @Test
    public void ironBankUsesScriptsDirectories() {
        DockerVariant ironBank = new DockerVariant(Architecture.X64, DockerBase.IRON_BANK);
        assertEquals("scripts", ironBank.configDirectory());
        assertEquals("scripts", ironBank.binDirectory());
        DockerVariant defaultVariant = new DockerVariant(Architecture.X64, DockerBase.DEFAULT);
        assertEquals("config", defaultVariant.configDirectory());
        assertEquals("bin", defaultVariant.binDirectory());
    }

    @Test
    public void buildDateIsTruncatedOutsideCi() {
        ZonedDateTime buildDate = ZonedDateTime.of(2026, 10, 12, 13, 14, 15, 0, ZoneOffset.UTC);
        assertEquals("2026-10-12T13:14:15Z", DockerVariant.buildDate(buildDate, true));
        assertEquals("2026-10-12T00:00Z", DockerVariant.buildDate(buildDate, false));
    }

    @Test
    public void templateExpansions() {
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("arch", "aarch64");
        expected.put("base_image", "${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}");
        expected.put("bin_dir", "scripts");
        expected.put("build_date", "2026-10-12T00:00Z");
        expected.put("config_dir", "scripts");
        expected.put("git_revision", "abc123");
        expected.put("license", "Elastic License 2.0");
        expected.put("package_manager", "yum");
        expected.put("docker_base", "iron_bank");
        expected.put("docker_context", "docker-build-context");
        expected.put("version", VERSION);
        expected.put("major_minor_version", "9.3");
        expected.put("retry", ShellRetry.class);
        expected.put("bc_fips_version", "2.0.2");

        Map<String, Object> actual = new DockerVariant(Architecture.AARCH64, DockerBase.IRON_BANK).templateExpansions(
            VERSION,
            "2026-10-12T00:00Z",
            "abc123",
            "2.0.2",
            "docker-build-context"
        );
        assertEquals(expected, actual);
        assertEquals(List.copyOf(expected.keySet()), List.copyOf(actual.keySet()));
    }

    @Test
    public void templateExpansionsForProgrammaticBase() {
        Map<String, Object> expansions = new DockerVariant(Architecture.X64, DockerBase.WOLFI).templateExpansions(
            VERSION,
            "2026-10-12T00:00Z",
            "abc123",
            "2.0.2",
            ""
        );
        assertEquals(null, expansions.get("base_image"));
        assertEquals("Elastic-License-2.0", expansions.get("license"));
        assertEquals("bin", expansions.get("bin_dir"));
        assertEquals("wolfi", expansions.get("docker_base"));
        assertEquals("cloud_ess", new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS).dockerBaseName());
    }

    @Test
    public void exportProjectNames() {
        Map<String, DockerVariant> expected = new LinkedHashMap<>();
        expected.put("docker-export", new DockerVariant(Architecture.X64, DockerBase.DEFAULT));
        expected.put("docker-aarch64-export", new DockerVariant(Architecture.AARCH64, DockerBase.DEFAULT));
        expected.put("ironbank-docker-export", new DockerVariant(Architecture.X64, DockerBase.IRON_BANK));
        expected.put("ironbank-docker-aarch64-export", new DockerVariant(Architecture.AARCH64, DockerBase.IRON_BANK));
        expected.put("wolfi-docker-export", new DockerVariant(Architecture.X64, DockerBase.WOLFI));
        expected.put("wolfi-docker-aarch64-export", new DockerVariant(Architecture.AARCH64, DockerBase.WOLFI));
        expected.put("cloud-ess-docker-export", new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS));
        expected.put("cloud-ess-docker-aarch64-export", new DockerVariant(Architecture.AARCH64, DockerBase.CLOUD_ESS));
        expected.put("cloud-ess-fips-docker-export", new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS_FIPS));
        expected.put("cloud-ess-fips-docker-aarch64-export", new DockerVariant(Architecture.AARCH64, DockerBase.CLOUD_ESS_FIPS));

        assertEquals(DockerVariant.all().size(), expected.size());
        expected.forEach((projectName, variant) -> {
            assertEquals(projectName, variant.exportProjectName());
            assertEquals(variant, DockerVariant.fromExportProjectName(projectName));
        });
        assertThrows(IllegalArgumentException.class, () -> DockerVariant.fromExportProjectName("docker-s390x-export"));
    }

    @Test
    public void exportFileNames() {
        DockerVariant ironBank = new DockerVariant(Architecture.AARCH64, DockerBase.IRON_BANK);
        assertEquals("elasticsearch-aarch64-ironbank_test", ironBank.exportArtifactName());
        assertEquals("elasticsearch-aarch64-ironbank_test_" + VERSION + ".ironbank.tar", ironBank.exportFileName(VERSION));
        assertEquals("elasticsearch-ironbank-" + VERSION + "-docker-image", ironBank.exportArchiveBaseName(VERSION));

        DockerVariant defaultVariant = new DockerVariant(Architecture.X64, DockerBase.DEFAULT);
        assertEquals("elasticsearch_test", defaultVariant.exportArtifactName());
        assertEquals("elasticsearch_test_" + VERSION + ".docker.tar", defaultVariant.exportFileName(VERSION));

        assertEquals("cloud-ess-fips.tar", new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS_FIPS).exportExtension());
        assertEquals("cloud-ess.tar", new DockerVariant(Architecture.X64, DockerBase.CLOUD_ESS).exportExtension());
        assertEquals("wolfi.tar", new DockerVariant(Architecture.X64, DockerBase.WOLFI).exportExtension());
    }
}
