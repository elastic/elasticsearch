/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SpecReader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;

/**
 * Validates the consistency of {@code required_capability:} directives in csv-spec files
 * against the capabilities declared in {@link EsqlCapabilities}.
 *
 * <p>This lint runs unconditionally (snapshot and release builds, no cluster required) and
 * reports all violations at once — unlike the per-test assertion in
 * {@link org.elasticsearch.xpack.esql.CsvTestUtils#checkTestCapabilities}, which only fires
 * on snapshot builds and only as each test case executes inside {@code CsvIT}.</p>
 *
 * <p>Motivation: capability gaps (e.g. a csv-spec test referencing a misspelled or removed
 * capability, or using a capability that was never declared) cause {@code CsvIT} tests to be
 * silently skipped rather than visibly failing, making the problem easy to miss in PR review.</p>
 */
public class EsqlCapabilitiesLintTests extends ESTestCase {

    /**
     * Every {@code required_capability:} and {@code required_capability_coordinator:} value in
     * every csv-spec file must name a capability declared in
     * {@code EsqlCapabilities.capabilities(registry, true)}.
     *
     * <p>Violations indicate a typo in the csv-spec file, a capability that was renamed/removed
     * from {@link EsqlCapabilities} without updating the spec files, or a new test that
     * references a capability not yet declared in the enum.</p>
     *
     * <p>{@code missing_capability_coordinator:} and {@code missing_capability_data_node:} are
     * intentionally not checked: they may legitimately reference capabilities that have been
     * removed (a removed capability trivially satisfies the "missing" condition).</p>
     */
    public void testAllReferencedCapabilitiesAreDeclared() throws Exception {
        Set<String> allKnownCaps = EsqlCapabilities.capabilities(TEST_FUNCTION_REGISTRY, true).capabilities();

        List<URL> urls = EsqlTestUtils.classpathResources("/*.csv-spec");
        assertFalse("No csv-spec files found on the test classpath", urls.isEmpty());

        // "specFile:testName" -> unknown capability names (ordered for stable failure messages)
        TreeMap<String, List<String>> violations = new TreeMap<>();

        for (URL url : urls) {
            String urlStr = url.toString();
            String specFile = urlStr.substring(urlStr.lastIndexOf('/') + 1);

            List<Object[]> cases = SpecReader.readURLSpec(url, CsvSpecReader.specParser());
            for (Object[] row : cases) {
                String testName = (String) row[2];
                CsvSpecReader.CsvTestCase testCase = (CsvSpecReader.CsvTestCase) row[4];

                List<String> unknown = new ArrayList<>();
                for (String cap : testCase.requiredCapabilities) {
                    if (allKnownCaps.contains(cap) == false) {
                        unknown.add(cap);
                    }
                }
                for (String cap : testCase.requiredCapabilitiesLocalCluster) {
                    if (allKnownCaps.contains(cap) == false) {
                        unknown.add(cap + " (coordinator-only)");
                    }
                }

                if (unknown.isEmpty() == false) {
                    violations.put(specFile + ":" + testName, unknown);
                }
            }
        }

        if (violations.isEmpty() == false) {
            StringBuilder msg = new StringBuilder(
                "csv-spec files reference capabilities not declared in EsqlCapabilities"
                    + " (typo, renamed/removed capability, or missing declaration):\n"
            );
            for (var entry : violations.entrySet()) {
                msg.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
            msg.append("Fix: correct the capability name in the csv-spec file," + " or add a new entry to EsqlCapabilities.Cap.");
            fail(msg.toString());
        }
    }
}
