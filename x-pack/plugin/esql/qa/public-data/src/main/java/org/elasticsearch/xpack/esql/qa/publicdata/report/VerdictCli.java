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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The pipeline's merge step: combines per-shard JUnit XML artifacts into one verdict. Exits 0
 * only on PASS, so the merge step's exit code IS the run's pass/fail.
 *
 * <p>Args: repeated {@code --results <dir>} (per-shard test-results directories),
 * optional {@code --pin-report <pin-report.json>} from the pre-step, {@code --output <dir>}.
 */
public final class VerdictCli {

    private VerdictCli() {}

    @SuppressForbidden(reason = "CLI tool reports to stdout/stderr and sets an exit code")
    public static void main(String[] args) throws IOException {
        List<Path> resultDirs = new ArrayList<>();
        Path pinReport = null;
        Path outputDir = Path.of("build/public-data-results");
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--results" -> resultDirs.add(Path.of(args[i + 1]));
                case "--pin-report" -> pinReport = Path.of(args[i + 1]);
                case "--output" -> outputDir = Path.of(args[i + 1]);
                default -> {
                    /* value position */ }
            }
        }
        if (resultDirs.isEmpty()) {
            System.err.println("usage: --results <test-results-dir> [--results <dir>]... [--pin-report <json>] [--output <dir>]");
            System.exit(2);
        }

        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        Map<String, WorkloadSpec> workloads = new LinkedHashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() != null) {
                workloads.put(corpus.workload(), WorkloadSpec.loadFromClasspath(corpus.workload()));
            }
        }

        List<JUnitResults.TestResult> observed = new ArrayList<>();
        for (Path dir : resultDirs) {
            observed.addAll(JUnitResults.parse(dir));
        }

        Set<String> driftLabels = new HashSet<>();
        if (pinReport != null && Files.exists(pinReport)) {
            // pin-report.json lines: {"leg": "<label>", "status": "<status>"}
            for (String line : Files.readAllLines(pinReport, StandardCharsets.UTF_8)) {
                if (line.contains("\"PIN_DRIFT\"")) {
                    int start = line.indexOf("\"leg\": \"") + 8;
                    driftLabels.add(line.substring(start, line.indexOf('"', start)));
                }
            }
        }

        Verdict verdict = Verdict.evaluate(catalog, workloads, observed, driftLabels);
        Files.createDirectories(outputDir);
        Files.writeString(outputDir.resolve("verdict.json"), VerdictWriter.toJson(verdict), StandardCharsets.UTF_8);
        Files.writeString(outputDir.resolve("verdict-annotation.md"), VerdictWriter.toAnnotationMarkdown(verdict), StandardCharsets.UTF_8);
        System.out.println("verdict: " + verdict.status() + " (" + verdict.legs().size() + " legs)");
        verdict.problems().forEach(p -> System.out.println("  problem: " + p));
        if (verdict.status() != Verdict.Status.PASS) {
            System.exit(1);
        }
    }
}
