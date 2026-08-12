/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

/**
 * Runs csv-spec tests in a mixed-version cluster with an old-version node as coordinator. This catches
 * regressions where a new-version data node sends a capability-gated response fragment to an old coordinator
 * that cannot deserialize it. {@link AbstractMixedClusterEsqlCurrCoordSpecIT} is the counterpart for
 * the current version; all shared behaviour lives in {@link AbstractMixedClusterEsqlSpecIT}.
 */
public abstract class AbstractMixedClusterEsqlOldCoordSpecIT extends AbstractMixedClusterEsqlSpecIT {
    protected AbstractMixedClusterEsqlOldCoordSpecIT(
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
     * Routes queries through old-version nodes only, selected from {@code GET /_nodes} by matching
     * {@code tests.old_cluster_version}. Including a current-version address would let a new node coordinate
     * and silently make this suite overlap {@link AbstractMixedClusterEsqlCurrCoordSpecIT}.
     */
    @Override
    protected String getTestRestCluster() {
        return httpAddressesForCoordinator(true);
    }
}
