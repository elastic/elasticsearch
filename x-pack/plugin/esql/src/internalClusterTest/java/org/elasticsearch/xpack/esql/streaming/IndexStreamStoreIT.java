/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IndexStreamStoreIT extends AbstractEsqlIntegTestCase {

    private IndexStreamStore store;

    @Before
    public void createStore() {
        store = new IndexStreamStore(client());
    }

    public void testCreateAndUpdateMetadata() {
        StreamMetadata metadata = createRamdomStartedStream();

        ESTestCase.<Void>safeAwait(l -> store.create(metadata, l));
        StreamMetadata stored = safeAwait(l -> store.getMetadata(metadata.streamId(), l));
        assertThat(stored, equalTo(metadata));

        for (var updated : List.of(
            metadata.withExpiration(randomFutureExpiration()),
            metadata.canceled(),
            metadata.complete(randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            metadata.failed(randomBoolean(), randomNonNegativeLong(), new RuntimeException("simulated"))
        )) {
            ESTestCase.<Void>safeAwait(l -> store.update(updated, l));
            stored = safeAwait(l -> store.getMetadata(metadata.streamId(), l));
            assertThat(stored, equalTo(updated));
        }
    }

    private static StreamMetadata createRamdomStartedStream() {
        return StreamMetadata.started(
            randomUUID(),
            randomTaskId(),
            randomFutureExpiration(),
            randomList(1, 5, IndexStreamStoreIT::randomColumn),
            randomMap(1, 5, () -> Tuple.tuple(randomIdentifier(), randomIdentifier())),
            List.of()
        );
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomIdentifier(), randomNonNegativeLong());
    }

    private static Instant randomFutureExpiration() {
        return Instant.now().plusSeconds(randomIntBetween(30, 3600)).truncatedTo(ChronoUnit.MILLIS);
    }

    private static ColumnInfoImpl randomColumn() {
        return new ColumnInfoImpl(randomIdentifier(), randomFrom("integer", "long", "keyword", "double", "boolean"), null);
    }
}
