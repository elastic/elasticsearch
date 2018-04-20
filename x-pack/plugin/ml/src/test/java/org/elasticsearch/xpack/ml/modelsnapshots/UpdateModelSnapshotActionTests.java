/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.modelsnapshots;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;


public class UpdateModelSnapshotActionTests extends ESTestCase {

    public void testUpdateDescription_GivenMissingArg() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new UpdateModelSnapshotAction.Request(null, "foo"));
        assertEquals("[job_id] must not be null.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> new UpdateModelSnapshotAction.Request("foo", null));
        assertEquals("[snapshot_id] must not be null.", e.getMessage());
    }
}
