/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NativeMemoryCapacityTests extends ESTestCase {

    public void testMerge() {
        NativeMemoryCapacity capacity = new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(200).getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
            );
        capacity.merge(new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(100).getBytes()));
        assertThat(capacity.getTier(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 2L));
        assertThat(capacity.getNode(), equalTo(ByteSizeValue.ofMb(200).getBytes()));
        assertThat(capacity.getJvmSize(), equalTo(ByteSizeValue.ofMb(50).getBytes()));

        capacity.merge(new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(300).getBytes()));

        assertThat(capacity.getTier(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 3L));
        assertThat(capacity.getNode(), equalTo(ByteSizeValue.ofMb(300).getBytes()));
        assertThat(capacity.getJvmSize(), is(nullValue()));
    }


    public void testAutoscalingCapacity() {
        // TODO adjust once future JVM capacity is known
        NativeMemoryCapacity capacity = new NativeMemoryCapacity(
            ByteSizeValue.ofGb(4).getBytes(),
            ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
        );

        { // auto is false
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, false);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 4L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(ByteSizeValue.ofGb(4).getBytes() * 4L));
        }
        { // auto is true
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, true);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(1412818190L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(5651272758L));
        }
        { // auto is true with unknown jvm size
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes(),
                ByteSizeValue.ofGb(1).getBytes()
            );
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, true);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(2618882498L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(10475529991L));
        }

    }


}
