/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import java.io.IOException;

/** Tests indexed flattened fields with a suite timeout independent of the unindexed scenario. */
public class FlattenedRollingUpgradeIT extends AbstractFlattenedRollingUpgradeTestCase {
    public void testIndexing() throws IOException {
        assertIndexing(true);
    }
}
