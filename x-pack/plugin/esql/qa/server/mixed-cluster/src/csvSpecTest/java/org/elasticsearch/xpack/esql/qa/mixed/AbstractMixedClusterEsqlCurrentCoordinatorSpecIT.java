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
 * {@link AbstractMixedClusterEsqlOldCoordinatorSpecIT} is the counterpart for the old version; all shared
 * behaviour lives in {@link AbstractMixedClusterEsqlSpecIT}.
 */
public abstract class AbstractMixedClusterEsqlCurrentCoordinatorSpecIT extends AbstractMixedClusterEsqlSpecIT {
    protected AbstractMixedClusterEsqlCurrentCoordinatorSpecIT(
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
     * Nodes 1 and 3 are the current-version ones, because {@link Clusters#mixedVersionCluster} declares its
     * nodes as old, current, old, current. Including an old-version address here would let an old node
     * coordinate and silently make this suite overlap {@link AbstractMixedClusterEsqlOldCoordinatorSpecIT}.
     */
    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(1) + "," + cluster.getHttpAddress(3);
    }
}
