/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

/**
 * Runs csv-spec tests in a mixed-version cluster with a current-version node as coordinator.
 * {@link AbstractMixedClusterEsqlOldCoordSpecIT} is the counterpart for the old version; all shared
 * behaviour lives in {@link AbstractMixedClusterEsqlSpecIT}.
 */
public abstract class AbstractMixedClusterEsqlCurrCoordSpecIT extends AbstractMixedClusterEsqlSpecIT {
    protected AbstractMixedClusterEsqlCurrCoordSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    /**
     * Routes queries through current-version nodes only, selected from {@code GET /_nodes} as every node whose
     * version does not match {@code tests.old_cluster_version}. Including an old-version address would let an
     * old node coordinate and silently make this suite overlap {@link AbstractMixedClusterEsqlOldCoordSpecIT}.
     */
    @Override
    protected String getTestRestCluster() {
        return httpAddressesForCoordinator(false);
    }
}
