/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

public class DeleteModelSnapshotRequestTests extends ESTestCase {

    public void test_WithNullJobId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () ->
            new DeleteModelSnapshotRequest(null, randomAlphaOfLength(10)));
        assertEquals("[job_id] must not be null", ex.getMessage());
    }

    public void test_WithNullSnapshotId() {
        NullPointerException ex = expectThrows(NullPointerException.class, ()
            -> new DeleteModelSnapshotRequest(randomAlphaOfLength(10), null));
        assertEquals("[snapshot_id] must not be null", ex.getMessage());
    }
}
