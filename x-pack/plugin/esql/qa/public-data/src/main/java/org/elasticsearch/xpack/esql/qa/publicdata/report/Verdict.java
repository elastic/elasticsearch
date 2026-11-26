/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The scheduled run's machine-readable pass/fail with attributable per-leg results, evaluated
 * against the catalog-derived expectation. The threshold (agreed in review):
 *
 * <p><b>PASS</b> iff (i) the executed-leg count equals the catalog-derived expectation <b>and is
 * &gt; 0</b> — a silently skipped or self-disabled task can therefore never produce a green run;
 * (ii) every executed leg passes; and (iii) every non-executed cell is {@code blocked}, a declared
 * {@code gap}, or defect-disabled. A correctness mismatch is an unconditional <b>FAIL</b>. A
 * third-party transport failure after retry exhaustion is <b>INFRA_FAIL</b>: it fails the run,
 * attributed separately so it cannot be read as a regression. A pin mismatch found by the
 * pre-step marks that corpus's legs <b>PIN_DRIFT</b>: they are not executed for correctness, and
 * the run fails labelled <i>maintenance</i>. There is no oracle term here: the oracle established
 * the expectations at authoring time and does not run in the pipeline.
 */
public record Verdict(Status status, List<LegResult> legs, List<String> problems) {

    public enum Status {
        PASS,
        /** A frozen expectation was violated: a real regression signal. */
        FAIL,
        /** Retries exhausted against a third-party store; attributed, still red. */
        INFRA_FAIL,
        /** Upstream bytes moved (pin drift): re-pin, re-derive, re-review. */
        MAINTENANCE
    }

    /** One leg = one catalog variant's execution summary. */
    public record LegResult(
        String corpusId,
        String label,
        String outcome, // PASS | FAIL | INFRA_FAIL | PIN_DRIFT | NOT_EXECUTED
        int expectedTests,
        int executedTests,
        int failedTests,
        List<String> failures
    ) {}

    /**
     * Evaluates observed results against the catalog. {@code pinDriftLabels} come from the
     * pipeline's pin pre-step; their legs are expected NOT to have run.
     */
    public static Verdict evaluate(
        PublicDataCatalog catalog,
        Map<String, WorkloadSpec> workloads,
        List<JUnitResults.TestResult> observed,
        Set<String> pinDriftLabels
    ) {
        Map<String, List<JUnitResults.TestResult>> byLabel = new HashMap<>();
        for (JUnitResults.TestResult result : observed) {
            byLabel.computeIfAbsent(result.variantLabel(), k -> new ArrayList<>()).add(result);
        }

        List<LegResult> legs = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        boolean anyCorrectnessFailure = false;
        boolean anyInfraFailure = false;
        boolean anyDrift = false;
        int totalExpected = 0;
        int totalExecuted = 0;

        for (CorpusSpec corpus : catalog.corpora()) {
            for (VariantSpec variant : corpus.variants()) {
                if (variant.active() == false) {
                    continue; // backup entries and defect-disabled variants are not expected to run
                }
                WorkloadSpec workload = corpus.workload() == null ? null : workloads.get(corpus.workload());
                int expected = expectedTests(corpus, variant, workload);
                List<JUnitResults.TestResult> results = byLabel.getOrDefault(variant.label(), List.of());
                int executed = (int) results.stream().filter(r -> r.status() != JUnitResults.Status.SKIPPED).count();
                List<String> failures = results.stream()
                    .filter(r -> r.status() == JUnitResults.Status.FAILED)
                    .map(r -> r.testName() + ": " + (r.failureMessage() == null ? "" : r.failureMessage()))
                    .toList();
                boolean infraOnly = failures.isEmpty() == false && failures.stream().allMatch(f -> f.contains("INFRA_FAIL: "));

                String outcome;
                if (pinDriftLabels.contains(variant.label())) {
                    outcome = "PIN_DRIFT";
                    anyDrift = true;
                    if (executed > 0) {
                        problems.add("leg [" + variant.label() + "] ran despite PIN_DRIFT; its results are not trustworthy");
                    }
                } else if (executed == 0) {
                    outcome = "NOT_EXECUTED";
                    problems.add("leg [" + variant.label() + "] expected " + expected + " tests but none executed");
                    anyInfraFailure = true; // an unexecuted expected leg can never be green
                } else {
                    totalExpected += expected;
                    totalExecuted += executed;
                    if (failures.isEmpty()) {
                        outcome = "PASS";
                        if (executed != expected) {
                            problems.add(
                                "leg [" + variant.label() + "] executed " + executed + " tests but the catalog expects " + expected
                            );
                            anyInfraFailure = true;
                        }
                    } else if (infraOnly) {
                        outcome = "INFRA_FAIL";
                        anyInfraFailure = true;
                    } else {
                        outcome = "FAIL";
                        anyCorrectnessFailure = true;
                    }
                }
                legs.add(new LegResult(corpus.id(), variant.label(), outcome, expected, executed, failures.size(), failures));
            }
        }

        for (String label : byLabel.keySet()) {
            if (legs.stream().noneMatch(l -> l.label().equals(label))) {
                problems.add("results contain tests for unknown/inactive variant [" + label + "]");
            }
        }

        Status status;
        if (anyCorrectnessFailure) {
            status = Status.FAIL;
        } else if (anyInfraFailure) {
            status = Status.INFRA_FAIL;
        } else if (anyDrift) {
            status = Status.MAINTENANCE;
        } else if (totalExecuted == 0 || totalExecuted != totalExpected) {
            status = Status.INFRA_FAIL;
            problems.add("executed test count [" + totalExecuted + "] != catalog-derived expectation [" + totalExpected + "] (or zero)");
        } else {
            status = Status.PASS;
        }
        return new Verdict(status, List.copyOf(legs), List.copyOf(problems));
    }

    /** The catalog-derived expectation for one variant, mirroring PublicDataIT's enumeration. */
    static int expectedTests(CorpusSpec corpus, VariantSpec variant, WorkloadSpec workload) {
        if (variant.expectFailure() != null) {
            return 1;
        }
        if (workload == null) {
            return 0;
        }
        return (int) workload.tests()
            .stream()
            .filter(t -> t.disabled() == false)
            .filter(t -> variant.querySubset().isEmpty() || variant.querySubset().contains(t.baseName()))
            .count();
    }
}
