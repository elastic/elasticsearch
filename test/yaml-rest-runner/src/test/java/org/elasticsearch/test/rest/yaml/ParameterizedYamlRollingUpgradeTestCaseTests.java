/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class ParameterizedYamlRollingUpgradeTestCaseTests extends ESTestCase {

    private static final String[] ALL = { "0 suite1/10_basic", "0 suite1/20_another_test", "1 suite2/10_basic", "1 suite2/15_test2" };

    public void testNoFilterRunsEveryPhase() throws Exception {
        assertThat(phaseSuites(null), contains(ALL));
    }

    public void testSuiteFilterRunsSameNamedSuiteInEveryPhase() throws Exception {
        assertThat(phaseSuites("suite2/10_basic"), contains("0 suite1/10_basic", "1 suite2/10_basic"));
    }

    public void testPhaseDirectoryInFilterRunsEveryPhase() throws Exception {
        assertThat(phaseSuites("suite2/10_basic,suite2"), contains(ALL));
    }

    private static List<String> phaseSuites(String scoped) throws Exception {
        return ParameterizedYamlRollingUpgradeTestCase.phaseParameters(scoped, "suite1", "suite2")
            .stream()
            .map(row -> row[0] + " " + ((ClientYamlTestCandidate) row[1]).getSuitePath())
            .toList();
    }
}
