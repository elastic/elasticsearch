/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IngestModelMemoryProviderTests extends ESTestCase {

    @Override
    public void tearDown() throws Exception {
        IngestModelMemoryProvider.setInstance(IngestModelMemoryProvider.Noop.INSTANCE);
        super.tearDown();
    }

    public void testNoopProviderReturnsZeroExactBytes() {
        IngestModelMemoryProvider provider = IngestModelMemoryProvider.getInstance();
        IngestModelMemoryProvider.HeapRequirement requirement = provider.getRequiredHeapBytes();
        assertThat(requirement.heapBytes(), equalTo(0L));
        assertThat(requirement.isExact(), is(true));
    }

    public void testSetInstanceReplacesProvider() {
        IngestModelMemoryProvider custom = () -> new IngestModelMemoryProvider.HeapRequirement(100L, false);
        IngestModelMemoryProvider.setInstance(custom);
        IngestModelMemoryProvider.HeapRequirement requirement = IngestModelMemoryProvider.getInstance().getRequiredHeapBytes();
        assertThat(requirement.heapBytes(), equalTo(100L));
        assertThat(requirement.isExact(), is(false));
    }
}
