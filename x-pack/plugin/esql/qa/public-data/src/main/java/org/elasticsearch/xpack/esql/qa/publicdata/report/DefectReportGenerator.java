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
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * The suite's end deliverable: every Elasticsearch defect this exercise surfaced, with its
 * reproducer, collected from the two disable mechanisms — spec tests renamed {@code -Ignore} with
 * a {@code // defect:} block, and failure variants carrying a {@code disabled:} reason. Emits
 * {@code defects.md}, handed over for triage and issue filing. Nothing is fixed here.
 */
public final class DefectReportGenerator {

    private DefectReportGenerator() {}

    public static String generate(PublicDataCatalog catalog, Map<String, WorkloadSpec> workloads) {
        StringBuilder md = new StringBuilder("# Public-data suite: defect report\n\n");
        md.append("Every entry is exercised-and-known-broken: the query and its oracle-derived expected\n");
        md.append("table (or the failure case's declared expectation) stay in the suite as the reproducer.\n\n");
        int count = 0;

        for (CorpusSpec corpus : catalog.corpora()) {
            WorkloadSpec workload = corpus.workload() == null ? null : workloads.get(corpus.workload());
            if (workload != null) {
                for (WorkloadSpec.TestSpec test : workload.tests()) {
                    if (test.disabled() && test.provenance().containsKey("defect")) {
                        count++;
                        md.append("## ").append(count).append(". ").append(test.baseName()).append(" (").append(corpus.id()).append(")\n");
                        md.append("- **defect:** ").append(test.provenance().get("defect")).append('\n');
                        appendIfPresent(md, test.provenance(), "defect-variant", "affected variant");
                        appendIfPresent(md, test.provenance(), "defect-oracle", "oracle vs ES|QL");
                        appendIfPresent(md, test.provenance(), "defect-found", "found");
                        appendIfPresent(md, test.provenance(), "oracle-sql", "oracle SQL");
                        md.append("- **reproducer:** `").append(workload.fileName()).append("` test `").append(test.name()).append("`\n");
                        md.append("- **query:** `").append(test.query().replace("\n", " ")).append("`\n\n");
                    }
                }
            }
            for (VariantSpec variant : corpus.variants()) {
                if (variant.expectFailure() != null && variant.disabledReason() != null) {
                    count++;
                    md.append("## ").append(count).append(". ").append(variant.label()).append(" (failure case)\n");
                    md.append("- **defect:** ").append(variant.disabledReason().strip().replace("\n", " ")).append('\n');
                    md.append("- **declared expectation:** ")
                        .append(variant.expectFailure().status())
                        .append(" matching `")
                        .append(variant.expectFailure().messageRegex())
                        .append("` — ")
                        .append(variant.expectFailure().reason().strip().replace("\n", " "))
                        .append('\n');
                    md.append("- **reproducer:** dataset over `").append(variant.resource()).append("`\n\n");
                }
            }
        }
        md.append(count == 0 ? "No defects currently on file.\n" : "Total: " + count + " defect(s).\n");
        return md.toString();
    }

    private static void appendIfPresent(StringBuilder md, Map<String, String> provenance, String key, String title) {
        if (provenance.containsKey(key)) {
            md.append("- **").append(title).append(":** ").append(provenance.get(key)).append('\n');
        }
    }

    @SuppressForbidden(reason = "CLI tool reports to stdout")
    public static void main(String[] args) throws IOException {
        Path outputDir = Path.of(args.length >= 2 && args[0].equals("--output") ? args[1] : "build/public-data-results");
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        Map<String, WorkloadSpec> workloads = new java.util.LinkedHashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() != null) {
                workloads.put(corpus.workload(), WorkloadSpec.loadFromClasspath(corpus.workload()));
            }
        }
        Files.createDirectories(outputDir);
        Path file = outputDir.resolve("defects.md");
        Files.writeString(file, generate(catalog, workloads), StandardCharsets.UTF_8);
        System.out.println("wrote " + file);
    }
}
