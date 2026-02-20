/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetSampleConfigurationActionResponseTests extends AbstractWireSerializingTestCase<GetSampleConfigurationAction.Response> {

    @Override
    protected Writeable.Reader<GetSampleConfigurationAction.Response> instanceReader() {
        return GetSampleConfigurationAction.Response::new;
    }

    @Override
    protected GetSampleConfigurationAction.Response createTestInstance() {
        String index = randomAlphaOfLengthBetween(1, 20);
        SamplingConfiguration config = randomBoolean() ? createRandomSamplingConfiguration() : null;
        return new GetSampleConfigurationAction.Response(index, config);
    }

    @Override
    protected GetSampleConfigurationAction.Response mutateInstance(GetSampleConfigurationAction.Response instance) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new GetSampleConfigurationAction.Response(
                randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(1, 20)),
                instance.getConfiguration()
            );
            case 1 -> new GetSampleConfigurationAction.Response(
                instance.getIndex(),
                randomValueOtherThan(instance.getConfiguration(), () -> randomBoolean() ? createRandomSamplingConfiguration() : null)
            );
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    private SamplingConfiguration createRandomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.1, 1.0, true),
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            randomBoolean() ? null : ByteSizeValue.ofKb(randomIntBetween(50, 100)),
            randomBoolean() ? TimeValue.timeValueMinutes(randomIntBetween(1, 60)) : null,
            randomBoolean() ? "ctx?.field == 'test'" : null
        );
    }
}
