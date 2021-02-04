/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for testing {@link RollupMetadata}, the Rollup V2 cluster metadata
 */
public class RollupMetadataTests extends AbstractNamedWriteableTestCase<RollupMetadata> {

    @Override
    protected RollupMetadata createTestInstance() {
        if (randomBoolean()) {
            return new RollupMetadata(Collections.emptyMap());
        }
        Map<String, RollupGroup> rollupGroups = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            rollupGroups.put(randomAlphaOfLength(5), RollupGroupTests.randomInstance());
        }
        return new RollupMetadata(rollupGroups);
    }

    @Override
    protected RollupMetadata mutateInstance(RollupMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(RollupMetadata.class,
            RollupMetadata.TYPE, RollupMetadata::new)));
    }

    @Override
    protected Class<RollupMetadata> categoryClass() {
        return RollupMetadata.class;
    }
}
