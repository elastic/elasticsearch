/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.modelsnapshots;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;

public class GetModelSnapshotsTests extends ESTestCase {

    public void testModelSnapshots_GivenNegativeFrom() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GetModelSnapshotsAction.Request("foo", null).setPageParams(new PageParams(-5, 10)));
        assertEquals("Parameter [from] cannot be < 0", e.getMessage());
    }

    public void testModelSnapshots_GivenNegativeSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GetModelSnapshotsAction.Request("foo", null).setPageParams(new PageParams(10, -5)));
        assertEquals("Parameter [size] cannot be < 0", e.getMessage());
    }
}
