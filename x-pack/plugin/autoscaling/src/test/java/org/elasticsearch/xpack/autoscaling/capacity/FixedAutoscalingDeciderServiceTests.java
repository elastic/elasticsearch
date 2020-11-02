/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.hamcrest.Matchers;

public class FixedAutoscalingDeciderServiceTests extends AutoscalingTestCase {
    public void testScale() {
        FixedAutoscalingDeciderConfiguration configuration = new FixedAutoscalingDeciderConfiguration(
            null,
            null,
            randomFrom(randomIntBetween(1, 1000), null)
        );
        verify(configuration, null);

        ByteSizeValue storage = randomNullableByteSizeValue();
        ByteSizeValue memory = storage != null ? randomNullableByteSizeValue() : randomByteSizeValue();
        verify(
            new FixedAutoscalingDeciderConfiguration(storage, memory, null),
            AutoscalingCapacity.builder().node(storage, memory).total(storage, memory).build()
        );

        int nodes = randomIntBetween(1, 1000);
        verify(
            new FixedAutoscalingDeciderConfiguration(storage, memory, nodes),
            AutoscalingCapacity.builder().node(storage, memory).total(multiply(storage, nodes), multiply(memory, nodes)).build()
        );
    }

    private void verify(FixedAutoscalingDeciderConfiguration configuration, AutoscalingCapacity expected) {
        FixedAutoscalingDeciderService service = new FixedAutoscalingDeciderService();
        AutoscalingDeciderResult result = service.scale(configuration, null);
        assertThat(result.requiredCapacity(), Matchers.equalTo(expected));
    }

    private ByteSizeValue multiply(ByteSizeValue bytes, int nodes) {
        return bytes == null ? null : new ByteSizeValue(bytes.getBytes() * nodes);
    }
}
