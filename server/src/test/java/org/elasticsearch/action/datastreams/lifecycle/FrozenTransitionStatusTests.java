/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

public class FrozenTransitionStatusTests extends ESTestCase {

    public void testEnsureMetricOrdinalsOrder() {
        EnumSerializationTestUtils.assertEnumSerialization(
            FrozenTransitionStatus.class,
            FrozenTransitionStatus.WAITING,
            FrozenTransitionStatus.ELIGIBLE,
            FrozenTransitionStatus.MARKED,
            FrozenTransitionStatus.QUEUED,
            FrozenTransitionStatus.RUNNING,
            FrozenTransitionStatus.NOT_SUPPORTED
        );
    }

}
