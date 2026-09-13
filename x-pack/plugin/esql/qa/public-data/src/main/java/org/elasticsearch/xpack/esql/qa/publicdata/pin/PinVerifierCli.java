/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Entry point for the {@code verifyPublicDataPins} Gradle task — the pipeline's step 0. Emits a
 * per-corpus drift report ({@code pin-report.md}) consumed by the verdict step, and exits non-zero
 * when any pin drifted or a store was unreachable, so the pipeline can label the run maintenance
 * or infra rather than regression.
 */
public final class PinVerifierCli {

    private PinVerifierCli() {}

    @SuppressForbidden(reason = "CLI tool reports to stdout/stderr and sets an exit code")
    public static void main(String[] args) throws IOException {
        Path outputDir = Path.of(argValue(args, "--output", "build/public-data-results"));
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        List<PinVerifier.VariantResult> results = new PinVerifier().verify(catalog);

        StringBuilder report = new StringBuilder("# Public-data pin verification\n\n");
        report.append("| corpus | variant | status | details |\n|---|---|---|---|\n");
        boolean failed = false;
        for (PinVerifier.VariantResult result : results) {
            report.append("| ")
                .append(result.corpusId())
                .append(" | ")
                .append(result.label())
                .append(" | ")
                .append(result.status())
                .append(" | ")
                .append(String.join("; ", result.details()))
                .append(" |\n");
            System.out.println(
                String.format(Locale.ROOT, "%-12s %-55s %s", result.status(), result.label(), String.join("; ", result.details()))
            );
            failed |= result.status() == PinVerifier.Status.PIN_DRIFT || result.status() == PinVerifier.Status.UNREACHABLE;
        }
        Files.createDirectories(outputDir);
        Files.writeString(outputDir.resolve("pin-report.md"), report.toString(), StandardCharsets.UTF_8);
        // machine-readable twin for the verdict merge step: one JSON object per line
        StringBuilder json = new StringBuilder();
        for (PinVerifier.VariantResult result : results) {
            json.append("{\"leg\": \"").append(result.label()).append("\", \"status\": \"").append(result.status()).append("\"}\n");
        }
        Files.writeString(outputDir.resolve("pin-report.json"), json.toString(), StandardCharsets.UTF_8);
        if (failed) {
            System.err.println("pin verification failed; see " + outputDir.resolve("pin-report.md"));
            System.exit(1);
        }
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
