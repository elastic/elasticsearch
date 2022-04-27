/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FrozenShardsDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    FrozenShardsDeciderService.FrozenShardsReason> {

    @Override
    protected Writeable.Reader<FrozenShardsDeciderService.FrozenShardsReason> instanceReader() {
        return FrozenShardsDeciderService.FrozenShardsReason::new;
    }

    @Override
    protected FrozenShardsDeciderService.FrozenShardsReason createTestInstance() {
        return new FrozenShardsDeciderService.FrozenShardsReason(randomNonNegativeLong());
    }

    @Override
    protected FrozenShardsDeciderService.FrozenShardsReason mutateInstance(FrozenShardsDeciderService.FrozenShardsReason instance)
        throws IOException {
        return new FrozenShardsDeciderService.FrozenShardsReason(
            randomValueOtherThan(instance.shards(), ESTestCase::randomNonNegativeLong)
        );
    }
}
