/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.action.RollupShardIndexerStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardPersistentTaskState;

import java.io.IOException;
import java.util.List;

public class RollupShardPersistentTaskStateTests extends AbstractXContentSerializingTestCase<RollupShardPersistentTaskState> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskState.class,
                    RollupShardPersistentTaskState.NAME,
                    RollupShardPersistentTaskState::readFromStream
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<RollupShardPersistentTaskState> instanceReader() {
        return RollupShardPersistentTaskState::new;
    }

    @Override
    protected RollupShardPersistentTaskState createTestInstance() {
        try {
            return createRollupShardPersistentTaskState();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static RollupShardPersistentTaskState createRollupShardPersistentTaskState() throws IOException {
        return new RollupShardPersistentTaskState(
            randomFrom(RollupShardIndexerStatus.values()),
            new BytesRef(randomAlphaOfLengthBetween(10, 100))
        );
    }

    @Override
    protected RollupShardPersistentTaskState mutateInstance(RollupShardPersistentTaskState instance) throws IOException {
        return null; // TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected RollupShardPersistentTaskState doParseInstance(XContentParser parser) throws IOException {
        return RollupShardPersistentTaskState.fromXContent(parser);
    }
}
