/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.hamcrest.Matchers;

public class FixedAutoscalingDeciderServiceTests extends AutoscalingTestCase {
    public void testScale() {

        Settings.Builder configurationBuilder = Settings.builder();
        int nodes = randomIntBetween(1, 1000);
        if (randomBoolean()) {
            configurationBuilder.put(FixedAutoscalingDeciderService.NODES.getKey(), nodes);
        }
        verify(configurationBuilder.build(), null);

        configurationBuilder = Settings.builder();

        ByteSizeValue storage = randomNullableByteSizeValue();
        ByteSizeValue memory = storage != null ? randomNullableByteSizeValue() : randomByteSizeValue();
        if (storage != null) {
            configurationBuilder.put(FixedAutoscalingDeciderService.STORAGE.getKey(), storage);
        }
        if (memory != null) {
            configurationBuilder.put(FixedAutoscalingDeciderService.MEMORY.getKey(), memory);
        }
        verify(configurationBuilder.build(), AutoscalingCapacity.builder().node(storage, memory).total(storage, memory).build());

        configurationBuilder.put(FixedAutoscalingDeciderService.NODES.getKey(), nodes);
        verify(
            configurationBuilder.build(),
            AutoscalingCapacity.builder().node(storage, memory).total(multiply(storage, nodes), multiply(memory, nodes)).build()
        );

    }

    private void verify(Settings configuration, AutoscalingCapacity expected) {
        FixedAutoscalingDeciderService service = new FixedAutoscalingDeciderService();
        AutoscalingDeciderResult result = service.scale(configuration, null);
        assertThat(result.requiredCapacity(), Matchers.equalTo(expected));
    }

    private ByteSizeValue multiply(ByteSizeValue bytes, int nodes) {
        return bytes == null ? null : new ByteSizeValue(bytes.getBytes() * nodes);
    }
}
