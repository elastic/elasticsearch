/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Entry point for the {@code publicDataCoverageReport} Gradle task: writes {@code coverage.json}
 * and {@code coverage.md} derived from the shipped catalog and specs.
 */
public final class CoverageInventoryCli {

    private CoverageInventoryCli() {}

    @SuppressForbidden(reason = "CLI tool reports to stdout")
    public static void main(String[] args) throws IOException {
        Path outputDir = Path.of(argValue(args, "--output", "build/public-data-results"));
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        Map<String, WorkloadSpec> workloads = new LinkedHashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() != null) {
                workloads.put(corpus.workload(), WorkloadSpec.loadFromClasspath(corpus.workload()));
            }
        }
        CoverageInventory inventory = new CoverageInventory(catalog, workloads);
        Files.createDirectories(outputDir);
        Files.writeString(outputDir.resolve("coverage.json"), inventory.toJson(), StandardCharsets.UTF_8);
        Files.writeString(outputDir.resolve("coverage.md"), inventory.toMarkdown(), StandardCharsets.UTF_8);
        System.out.println("wrote " + outputDir.resolve("coverage.json") + " and " + outputDir.resolve("coverage.md"));
    }

    private static String argValue(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
