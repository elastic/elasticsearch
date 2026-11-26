/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import java.io.IOException;

/** Tests unindexed flattened fields with a suite timeout independent of the indexed scenario. */
public class FlattenedNoIndexRollingUpgradeIT extends AbstractFlattenedRollingUpgradeTestCase {
    public void testIndexingWithIndexFalse() throws IOException {
        assertIndexing(false);
    }
}
