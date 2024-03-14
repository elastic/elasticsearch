/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class AutoShardConditionTests extends AbstractWireSerializingTestCase<AutoShardCondition> {

    @Override
    protected Writeable.Reader<AutoShardCondition> instanceReader() {
        return AutoShardCondition::new;
    }

    @Override
    protected AutoShardCondition createTestInstance() {
        return new AutoShardCondition(randomNonNegativeInt());
    }

    @Override
    protected AutoShardCondition mutateInstance(AutoShardCondition instance) throws IOException {
        return new AutoShardCondition(randomValueOtherThan(instance.value, ESTestCase::randomNonNegativeInt));
    }
}
