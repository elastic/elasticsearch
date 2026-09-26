/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class BlockFactoryDefaultsTests extends ESTestCase {

    public void testLocalBreakerOverReserved() {
        ByteSizeValue heap4Gb = ByteSizeValue.ofGb(4);
        ByteSizeValue heap8Gb = ByteSizeValue.ofGb(8);
        ByteSizeValue smallHeap = ByteSizeValue.ofBytes(randomLongBetween(1, heap4Gb.getBytes() - 1));
        ByteSizeValue mediumHeap = ByteSizeValue.ofBytes(randomLongBetween(heap4Gb.getBytes() + 1, heap8Gb.getBytes() - 1));
        ByteSizeValue largeHeap = ByteSizeValue.ofBytes(randomLongBetween(heap8Gb.getBytes() + 1, ByteSizeValue.ofTb(1).getBytes()));
        // verify small heap
        {
            ByteSizeValue defaultReserved = BlockFactory.defaultLocalBreakerOverReserved(smallHeap);
            assertThat(defaultReserved, equalTo(ByteSizeValue.ofKb(8)));

            ByteSizeValue defaultMaxReserved = BlockFactory.defaultLocalBreakerMaxOverReserved(smallHeap);
            assertThat(defaultMaxReserved, equalTo(ByteSizeValue.ofKb(512)));
        }
        // verify 4gb heap
        {
            ByteSizeValue defaultReserved = BlockFactory.defaultLocalBreakerOverReserved(heap4Gb);
            assertThat(defaultReserved, equalTo(ByteSizeValue.ofKb(128)));

            ByteSizeValue defaultMaxReserved = BlockFactory.defaultLocalBreakerMaxOverReserved(heap4Gb);
            assertThat(defaultMaxReserved, equalTo(ByteSizeValue.ofMb(2)));
        }
        // verify medium heap
        {
            ByteSizeValue defaultReserved = BlockFactory.defaultLocalBreakerOverReserved(mediumHeap);
            assertThat(defaultReserved, equalTo(ByteSizeValue.ofKb(128)));

            ByteSizeValue defaultMaxReserved = BlockFactory.defaultLocalBreakerMaxOverReserved(mediumHeap);
            assertThat(defaultMaxReserved, equalTo(ByteSizeValue.ofMb(2)));
        }
        // verify 8Gb heap
        {
            ByteSizeValue defaultReserved = BlockFactory.defaultLocalBreakerOverReserved(heap8Gb);
            assertThat(defaultReserved, equalTo(ByteSizeValue.ofKb(512)));

            ByteSizeValue defaultMaxReserved = BlockFactory.defaultLocalBreakerMaxOverReserved(heap8Gb);
            assertThat(defaultMaxReserved, equalTo(ByteSizeValue.ofMb(4)));
        }
        // verify large heap
        {
            ByteSizeValue defaultReserved = BlockFactory.defaultLocalBreakerOverReserved(largeHeap);
            assertThat(defaultReserved, equalTo(ByteSizeValue.ofKb(512)));

            ByteSizeValue defaultMaxReserved = BlockFactory.defaultLocalBreakerMaxOverReserved(largeHeap);
            assertThat(defaultMaxReserved, equalTo(ByteSizeValue.ofMb(4)));
        }
    }
}
