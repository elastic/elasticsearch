/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class DownsampleShardPersistentTaskStateTests extends AbstractXContentSerializingTestCase<DownsampleShardPersistentTaskState> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskState.class,
                    DownsampleShardPersistentTaskState.NAME,
                    DownsampleShardPersistentTaskState::readFromStream
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<DownsampleShardPersistentTaskState> instanceReader() {
        return DownsampleShardPersistentTaskState::new;
    }

    @Override
    protected DownsampleShardPersistentTaskState createTestInstance() {
        try {
            return createRollupShardPersistentTaskState();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DownsampleShardPersistentTaskState createRollupShardPersistentTaskState() throws IOException {
        return new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.values()),
            new BytesRef(randomAlphaOfLengthBetween(10, 100))
        );
    }

    @Override
    protected DownsampleShardPersistentTaskState mutateInstance(DownsampleShardPersistentTaskState instance) throws IOException {
        return null; // TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected DownsampleShardPersistentTaskState doParseInstance(XContentParser parser) throws IOException {
        return DownsampleShardPersistentTaskState.fromXContent(parser);
    }
}
