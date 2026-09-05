/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

/**
 * Renders a {@link Verdict} as {@code verdict.json} (machine-readable) and as the Buildkite
 * annotation markdown (human triage: per-leg outcomes, problems, defect counts).
 */
public final class VerdictWriter {

    private VerdictWriter() {}

    public static String toJson(Verdict verdict) {
        StringBuilder json = new StringBuilder("{\n");
        json.append("  \"status\": \"").append(verdict.status()).append("\",\n");
        json.append("  \"legs\": [\n");
        for (int i = 0; i < verdict.legs().size(); i++) {
            Verdict.LegResult leg = verdict.legs().get(i);
            json.append("    {\"corpus\": \"")
                .append(leg.corpusId())
                .append("\", \"leg\": \"")
                .append(leg.label())
                .append("\", \"outcome\": \"")
                .append(leg.outcome())
                .append("\", \"expected\": ")
                .append(leg.expectedTests())
                .append(", \"executed\": ")
                .append(leg.executedTests())
                .append(", \"failed\": ")
                .append(leg.failedTests())
                .append("}")
                .append(i < verdict.legs().size() - 1 ? "," : "")
                .append('\n');
        }
        json.append("  ],\n  \"problems\": [\n");
        for (int i = 0; i < verdict.problems().size(); i++) {
            json.append("    \"")
                .append(escape(verdict.problems().get(i)))
                .append("\"")
                .append(i < verdict.problems().size() - 1 ? "," : "")
                .append('\n');
        }
        return json.append("  ]\n}\n").toString();
    }

    public static String toAnnotationMarkdown(Verdict verdict) {
        StringBuilder md = new StringBuilder();
        md.append("## Public-data suite verdict: **").append(verdict.status()).append("**\n\n");
        md.append("| corpus | leg | outcome | executed/expected | failures |\n|---|---|---|---|---|\n");
        for (Verdict.LegResult leg : verdict.legs()) {
            md.append("| ")
                .append(leg.corpusId())
                .append(" | ")
                .append(leg.label())
                .append(" | ")
                .append(leg.outcome())
                .append(" | ")
                .append(leg.executedTests())
                .append('/')
                .append(leg.expectedTests())
                .append(" | ")
                .append(leg.failedTests())
                .append(" |\n");
        }
        if (verdict.problems().isEmpty() == false) {
            md.append("\n**Problems:**\n");
            for (String problem : verdict.problems()) {
                md.append("- ").append(problem).append('\n');
            }
        }
        md.append("\nOutcome semantics: FAIL = frozen expectation violated (regression); INFRA_FAIL = store/transport");
        md.append(" trouble after bounded retries (attributed, still red); PIN_DRIFT = upstream bytes moved");
        md.append(" (maintenance: re-pin, re-derive, re-review); defect-disabled legs are exercised-and-known-broken");
        md.append(" and appear in defects.md, not here.\n");
        return md.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
