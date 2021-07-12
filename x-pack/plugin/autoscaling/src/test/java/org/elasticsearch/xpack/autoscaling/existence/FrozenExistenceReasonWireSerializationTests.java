/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.existence;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

public class FrozenExistenceReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    FrozenExistenceDeciderService.FrozenExistenceReason> {
    @Override
    protected Writeable.Reader<FrozenExistenceDeciderService.FrozenExistenceReason> instanceReader() {
        return FrozenExistenceDeciderService.FrozenExistenceReason::new;
    }

    @Override
    protected FrozenExistenceDeciderService.FrozenExistenceReason createTestInstance() {
        return new FrozenExistenceDeciderService.FrozenExistenceReason(randomList(between(0, 10), () -> randomAlphaOfLength(5)));
    }

    @Override
    protected FrozenExistenceDeciderService.FrozenExistenceReason mutateInstance(
        FrozenExistenceDeciderService.FrozenExistenceReason instance
    ) {
        List<String> indices = new ArrayList<>(instance.indices());
        if (indices.isEmpty() || randomBoolean()) {
            indices.add(randomAlphaOfLength(5));
        } else {
            indices.remove(between(0, indices.size() - 1));
        }
        return new FrozenExistenceDeciderService.FrozenExistenceReason(indices);
    }
}
