/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class OptimalShardCountConditionTests extends AbstractWireSerializingTestCase<OptimalShardCountCondition> {

    @Override
    protected Writeable.Reader<OptimalShardCountCondition> instanceReader() {
        return OptimalShardCountCondition::new;
    }

    @Override
    protected OptimalShardCountCondition createTestInstance() {
        return new OptimalShardCountCondition(randomNonNegativeInt());
    }

    @Override
    protected OptimalShardCountCondition mutateInstance(OptimalShardCountCondition instance) throws IOException {
        return new OptimalShardCountCondition(randomValueOtherThan(instance.value, ESTestCase::randomNonNegativeInt));
    }
}
