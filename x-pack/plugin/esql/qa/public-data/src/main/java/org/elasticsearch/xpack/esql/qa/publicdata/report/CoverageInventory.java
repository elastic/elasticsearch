/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.GapSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Derives the coverage inventory from the catalog: one row per corpus x variant cell with
 * {@code status in (covered, blocked, gap)} plus per-cell test counts and trim disclosure.
 * {@code blocked} is derived (structural impossibility, e.g. a multi-file layout on a provider
 * with no listing); {@code gap} rows come from the catalog's {@code gaps:} block, so every hole is
 * self-documenting. Trimmed legs are reported as {@code covered (subset: n/m)} — a trimmed leg
 * must never read as fully covered.
 */
public class CoverageInventory {

    /** One inventory row. */
    public record Cell(
        String corpusId,
        String label,
        String status,
        String detail,
        int tests,
        boolean crossValidated // whether the oracle read exactly this variant's bytes at authoring time
    ) {}

    private final PublicDataCatalog catalog;
    private final Map<String, WorkloadSpec> workloads;

    public CoverageInventory(PublicDataCatalog catalog, Map<String, WorkloadSpec> workloads) {
        this.catalog = catalog;
        this.workloads = workloads;
    }

    public List<Cell> cells() {
        List<Cell> cells = new ArrayList<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            WorkloadSpec workload = corpus.workload() == null ? null : workloads.get(corpus.workload());
            for (VariantSpec variant : corpus.variants()) {
                cells.add(cell(corpus, variant, workload));
            }
        }
        for (GapSpec gap : catalog.gaps()) {
            cells.add(new Cell("-", gap.id(), "gap", gap.reason() + " [" + String.join(", ", gap.cells()) + "]", 0, false));
        }
        return List.copyOf(cells);
    }

    private static Cell cell(CorpusSpec corpus, VariantSpec variant, WorkloadSpec workload) {
        if (variant.layout().multiFile() && variant.supportsGlob() == false) {
            return new Cell(corpus.id(), variant.label(), "blocked", "provider cannot list objects", 0, false);
        }
        if (variant.provider().active() == false) {
            return new Cell(corpus.id(), variant.label(), "gap", "provider not yet supported/active", 0, false);
        }
        if (variant.isBackup()) {
            return new Cell(corpus.id(), variant.label(), "gap", "backup entry; catalogued but not onboarded", 0, false);
        }
        if (variant.expectFailure() != null) {
            if (variant.disabledReason() != null) {
                return new Cell(corpus.id(), variant.label(), "covered", "defect-disabled: " + variant.disabledReason().strip(), 0, false);
            }
            return new Cell(corpus.id(), variant.label(), "covered", "expected-failure case", 1, false);
        }
        int totalTests = workload == null ? 0 : workload.tests().size();
        // count only the tests this leg actually runs: the subset trim applies before the disabled count
        List<WorkloadSpec.TestSpec> effective = workload == null
            ? List.of()
            : workload.tests()
                .stream()
                .filter(t -> variant.querySubset().isEmpty() || variant.querySubset().contains(t.baseName()))
                .toList();
        long disabledTests = effective.stream().filter(WorkloadSpec.TestSpec::disabled).count();
        StringBuilder detail = new StringBuilder();
        if (variant.querySubset().isEmpty() == false) {
            detail.append("subset: ").append(effective.size()).append('/').append(totalTests);
        }
        if (disabledTests > 0) {
            if (detail.length() > 0) {
                detail.append("; ");
            }
            detail.append(disabledTests).append(" test(s) defect-disabled");
        }
        if (detail.length() == 0) {
            detail.append("full workload");
        }
        String status = "covered";
        if (corpus.assertionMode() == CorpusSpec.AssertionMode.INVARIANT) {
            // An invariant-asserted leg is exercised but claims less than a frozen table, so it must
            // never read as fully covered -- the same no-silent-caps rule the subset trim obeys.
            status = "covered (invariant)";
            detail.append("; invariant assertions (upstream re-publishes these objects)");
        }
        return new Cell(corpus.id(), variant.label(), status, detail.toString(), effective.size(), variant.isReference());
    }

    /** Renders {@code coverage.md}. */
    public String toMarkdown() {
        StringBuilder md = new StringBuilder("# Public-data coverage inventory\n\n");
        md.append("| corpus | variant/gap | status | tests | cross-validated | detail |\n|---|---|---|---|---|---|\n");
        for (Cell cell : cells()) {
            md.append("| ")
                .append(cell.corpusId())
                .append(" | ")
                .append(cell.label())
                .append(" | ")
                .append(cell.status())
                .append(" | ")
                .append(cell.tests())
                .append(" | ")
                .append(cell.crossValidated() ? "yes" : "no")
                .append(" | ")
                .append(cell.detail())
                .append(" |\n");
        }
        return md.toString();
    }

    /** Renders {@code coverage.json} (hand-rolled: flat strings and ints only). */
    public String toJson() {
        StringBuilder json = new StringBuilder("{\n  \"cells\": [\n");
        List<Cell> cells = cells();
        for (int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            json.append("    {\"corpus\": \"")
                .append(cell.corpusId())
                .append("\", \"cell\": \"")
                .append(cell.label())
                .append("\", \"status\": \"")
                .append(cell.status())
                .append("\", \"tests\": ")
                .append(cell.tests())
                .append(", \"cross_validated\": ")
                .append(cell.crossValidated())
                .append(", \"detail\": \"")
                .append(cell.detail().replace("\"", "\\\""))
                .append("\"}")
                .append(i < cells.size() - 1 ? "," : "")
                .append('\n');
        }
        return json.append("  ]\n}\n").toString();
    }
}
