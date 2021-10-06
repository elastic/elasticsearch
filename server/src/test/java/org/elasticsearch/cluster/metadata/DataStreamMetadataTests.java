/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataStreamMetadataTests extends AbstractNamedWriteableTestCase<DataStreamMetadata> {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, ToXContent.EMPTY_PARAMS, DataStreamMetadata::fromXContent)
            .assertEqualsConsumer(this::assertEqualInstances)
            .test();
    }

    @Override
    protected DataStreamMetadata createTestInstance() {
        if (randomBoolean()) {
            return new DataStreamMetadata(Map.of(), Map.of());
        }
        Map<String, DataStream> dataStreams = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            dataStreams.put(randomAlphaOfLength(5), DataStreamTestHelper.randomInstance());
        }

        Map<String, DataStreamAlias> dataStreamsAliases = new HashMap<>();
        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                DataStreamAlias alias = DataStreamTestHelper.randomAliasInstance();
                dataStreamsAliases.put(alias.getName(), alias);
            }
        }
        return new DataStreamMetadata(dataStreams, dataStreamsAliases);
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
