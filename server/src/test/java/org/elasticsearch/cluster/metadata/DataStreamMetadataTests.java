/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataStreamMetadataTests extends AbstractChunkedSerializingTestCase<DataStreamMetadata> {

    @Override
    protected DataStreamMetadata createTestInstance() {
        if (randomBoolean()) {
            return new DataStreamMetadata(ImmutableOpenMap.of(), ImmutableOpenMap.of());
        }
        Map<String, DataStream> dataStreams = IntStream.range(0, randomIntBetween(1, 5))
            .boxed()
            .collect(Collectors.toUnmodifiableMap(i -> randomAlphaOfLength(5), i -> DataStreamTestHelper.randomInstance()));

        Map<String, DataStreamAlias> dataStreamsAliases = IntStream.range(0, randomIntBetween(1, 5))
            .mapToObj(i -> DataStreamTestHelper.randomAliasInstance())
            .collect(Collectors.toUnmodifiableMap(DataStreamAlias::getName, Function.identity()));

        return new DataStreamMetadata(ImmutableOpenMap.builder(dataStreams).build(), ImmutableOpenMap.builder(dataStreamsAliases).build());
    }

    @Override
    protected DataStreamMetadata mutateInstance(DataStreamMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(DataStreamMetadata.class, DataStreamMetadata.TYPE, DataStreamMetadata::new)
            )
        );
    }

    @Override
    protected DataStreamMetadata doParseInstance(XContentParser parser) throws IOException {
        return DataStreamMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStreamMetadata> instanceReader() {
        return DataStreamMetadata::new;
    }

    @Override
    protected boolean isFragment() {
        return true;
    }
}
