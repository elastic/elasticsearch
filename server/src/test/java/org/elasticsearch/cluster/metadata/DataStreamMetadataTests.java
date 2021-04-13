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

public class DataStreamMetadataTests extends AbstractNamedWriteableTestCase<DataStreamMetadata> {

    @Override
    protected DataStreamMetadata createTestInstance() {
        if (randomBoolean()) {
            return new DataStreamMetadata(Collections.emptyMap());
        }
        Map<String, DataStream> dataStreams = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            dataStreams.put(randomAlphaOfLength(5), DataStreamTestHelper.randomInstance());
        }
        return new DataStreamMetadata(dataStreams);
    }

    @Override
    protected DataStreamMetadata mutateInstance(DataStreamMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(DataStreamMetadata.class,
            DataStreamMetadata.TYPE, DataStreamMetadata::new)));
    }

    @Override
    protected Class<DataStreamMetadata> categoryClass() {
        return DataStreamMetadata.class;
    }
}
