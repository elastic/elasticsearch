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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

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
    protected DataStreamMetadata mutateInstance(DataStreamMetadata instance) {
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

    public void testWithAlias() {
        Index index1 = new Index("data-stream-1-index", "1");
        Index index2 = new Index("data-stream-2-index", "2");
        DataStream dataStream1 = new DataStream(
            "data-stream-1",
            List.of(index1),
            1,
            Map.of(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
        DataStream dataStream2 = new DataStream(
            "data-stream-2",
            List.of(index2),
            1,
            Map.of(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
        ImmutableOpenMap<String, DataStream> dataStreams = new ImmutableOpenMap.Builder<String, DataStream>().fPut(
            "data-stream-1",
            dataStream1
        ).fPut("data-stream-2", dataStream2).build();
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases = new ImmutableOpenMap.Builder<String, DataStreamAlias>().build();
        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(dataStreams, dataStreamAliases);
        dataStreamMetadata = dataStreamMetadata.withAlias("alias1", "data-stream-2", false, null);
        dataStreamMetadata = dataStreamMetadata.withAlias("alias1", "data-stream-1", false, """
            {
                "term": {
                    "reaction": "report"
                }
                }""");

        Map<String, DataStreamAlias> aliasMap = dataStreamMetadata.getDataStreamAliases();
        assertNotNull(aliasMap);
        assertThat(aliasMap.size(), equalTo(1));
        DataStreamAlias alias1 = aliasMap.get("alias1");
        assertNotNull(alias1);
        Map<String, DataStream> dataStreamMap = dataStreamMetadata.dataStreams();
        assertNotNull(dataStreamMap);
        assertThat(dataStreamMap.size(), equalTo(2));
        DataStream resultDataStream1 = dataStreamMap.get("data-stream-1");
        assertNotNull(resultDataStream1);
        DataStream resultDataStream2 = dataStreamMap.get("data-stream-2");
        assertNotNull(resultDataStream2);

        assertNotNull(alias1.getFilter(resultDataStream1.getName()));
        assertNull(alias1.getFilter(resultDataStream2.getName()));
    }
}
