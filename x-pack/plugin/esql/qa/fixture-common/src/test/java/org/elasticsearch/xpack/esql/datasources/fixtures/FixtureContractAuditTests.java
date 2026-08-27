/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The gate's teeth. Each test here mutates a declaration into a state the audit MUST reject; without
 * them the audit could pass every input and nothing would notice, which is the failure mode the audit
 * exists to prevent in the first place.
 */
public class FixtureContractAuditTests extends ESTestCase {

    private static Properties declaration(String... lines) {
        Properties props = new Properties();
        for (String line : lines) {
            int eq = line.indexOf('=');
            props.setProperty(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
        }
        return props;
    }

    /**
     * A cluster-bound value with no cluster wired is reachable through nothing. Left undeclared it is
     * indistinguishable from a value that works -- the vector simply never runs and no suite says so.
     */
    private static String[] unreachableCell() {
        return new String[] {
            "dimension.format.values = csv, parquet",
            "dimension.format.default = csv",
            "dimension.format.binds = fixture",
            "dimension.cluster_size.values = single, multi",
            "dimension.cluster_size.default = single",
            "dimension.cluster_size.binds = cluster",
            "pair.cluster_size.format = interacting" };
    }

    public void testAnUndeclaredAbsenceIsAViolation() {
        FixtureDimensions dimensions = FixtureDimensions.parse(declaration(unreachableCell()));
        List<FixtureContractAudit.Cell> cells = FixtureContractAudit.audit(dimensions);
        assertThat(FixtureContractAudit.violatingDimensions(cells), hasItem("cluster_size"));
    }

    /** Declaring why it cannot run is what clears it -- and the reason is carried into the report. */
    public void testATypedAbsenceClearsTheViolation() {
        String[] lines = ArrayUtils.append(
            unreachableCell(),
            "dimension.cluster_size.gap.multi = gap: no vector routes to the multi-node suite"
        );
        FixtureDimensions dimensions = FixtureDimensions.parse(declaration(lines));
        List<FixtureContractAudit.Cell> cells = FixtureContractAudit.audit(dimensions);
        assertThat(FixtureContractAudit.violatingDimensions(cells), not(hasItem("cluster_size")));
        assertThat(FixtureContractAudit.countByVerdict(cells).get("GAP"), equalTo(2L));
    }

    /**
     * Rot direction. An entry that outlives the absence it describes is worse than no entry: it reports
     * work still to do on something already done, so deleting it is how the seam's arrival is verified.
     */
    public void testAnAbsenceOnAReachableCellIsAViolation() {
        String[] lines = new String[] {
            "dimension.format.values = csv, parquet",
            "dimension.format.default = csv",
            "dimension.format.binds = fixture",
            "dimension.error_mode.values = fail_fast, skip_row",
            "dimension.error_mode.default = fail_fast",
            "dimension.error_mode.binds = directive",
            "dimension.error_mode.key = error_mode",
            "dimension.error_mode.gap.skip_row = gap: stale, this works now",
            "pair.error_mode.format = interacting" };
        FixtureDimensions dimensions = FixtureDimensions.parse(declaration(lines));
        List<FixtureContractAudit.Cell> cells = FixtureContractAudit.audit(dimensions);
        assertThat(FixtureContractAudit.violatingDimensions(cells), hasItem("error_mode"));
        // Two, not one: error_mode declares no applies_to, so the stale cell exists on every format.
        assertThat(FixtureContractAudit.countByVerdict(cells).get("STALE-ABSENCE"), equalTo(2L));
    }

    /**
     * The real contract. Every cell is reachable or carries a typed reason -- this is the assertion that
     * keeps a newly added dimension value from arriving unaccounted for.
     */
    public void testTheDeclaredContractHasNoUnaccountedCells() {
        List<FixtureContractAudit.Cell> cells = FixtureContractAudit.audit(FixtureDimensions.get());
        assertThat(FixtureContractAudit.violatingDimensions(cells), equalTo(Set.of()));
        assertThat("the contract is not empty", cells.isEmpty(), equalTo(false));
    }
}
