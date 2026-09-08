/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

/**
 * Runs S3/Parquet external csv-spec tests through an old-version coordinator.
 */
public abstract class AbstractMixedClusterExternalOldCoordSpecIT extends AbstractMixedClusterExternalSpecIT {

    protected AbstractMixedClusterExternalOldCoordSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected boolean oldCoordinator() {
        return true;
    }
}
