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
 * that cannot deserialize it. {@link AbstractMixedClusterEsqlCurrentCoordinatorSpecIT} is the counterpart for
 * the current version; all shared behaviour lives in {@link AbstractMixedClusterEsqlSpecIT}.
 */
public abstract class AbstractMixedClusterEsqlOldCoordinatorSpecIT extends AbstractMixedClusterEsqlSpecIT {
    protected AbstractMixedClusterEsqlOldCoordinatorSpecIT(
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
     * Nodes 0 and 2 are the old ones, because {@link Clusters#mixedVersionCluster} declares its nodes as old,
     * current, old, current. Including a current-version address here would let a new node coordinate and
     * silently make this suite overlap {@link AbstractMixedClusterEsqlCurrentCoordinatorSpecIT}.
     */
    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(0) + "," + cluster.getHttpAddress(2);
    }
}
