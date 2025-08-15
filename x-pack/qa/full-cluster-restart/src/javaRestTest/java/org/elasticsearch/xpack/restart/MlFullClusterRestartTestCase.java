/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.restart;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.junit.BeforeClass;

public abstract class MlFullClusterRestartTestCase extends AbstractXpackFullClusterRestartTestCase {

    protected static final boolean SKIP_ML_TESTS = Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));

    public MlFullClusterRestartTestCase(FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @BeforeClass
    public static void maybeSkip() {
        assumeFalse("Skip ML tests on unsupported glibc versions", SKIP_ML_TESTS);
    }
}
