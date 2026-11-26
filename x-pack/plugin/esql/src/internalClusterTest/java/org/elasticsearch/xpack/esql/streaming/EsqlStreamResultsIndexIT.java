/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor.INDEX;
import static org.hamcrest.Matchers.equalTo;

public class EsqlStreamResultsIndexIT extends AbstractEsqlIntegTestCase {

    public void testIndexMetadataDocument() throws IOException {
        var started = StreamMetadata.started(
            randomUUID(),
            new TaskId(randomIdentifier(), randomNonNegativeLong()),
            Instant.ofEpochMilli(System.currentTimeMillis()).plusSeconds(randomIntBetween(30, 3600)),
            randomList(
                1,
                5,
                () -> new ColumnInfoImpl(randomIdentifier(), randomFrom("integer", "long", "keyword", "double", "boolean"), null)
            ),
            randomMap(1, 5, () -> Tuple.tuple(randomIdentifier(), randomIdentifier())),
            randomList(0, 3, () -> randomAlphaOfLengthBetween(5, 30))
        );

        for (var metadata : List.of(
            started,
            started.canceled(),
            started.complete(randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            started.failed(randomBoolean(), randomNonNegativeLong(), new RuntimeException("test"))
        )) {
            client().index(StreamMetadataDocument.indexRequest(metadata)).actionGet();

            var response = client().prepareGet(INDEX, metadata.streamId()).setRouting(metadata.streamId()).get();
            assertTrue(response.isExists());
            assertThat(StreamMetadataDocument.fromSource(response.getSourceAsMap()), equalTo(metadata));
        }
    }
}
