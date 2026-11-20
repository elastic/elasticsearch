/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.StreamId;

import static org.hamcrest.Matchers.containsString;

/**
 * Runs the stream GET and DELETE transport actions on a real node. Nothing is stored yet, so no stream exists and every id is not found.
 */
public class StreamGetDeleteActionsIT extends AbstractEsqlIntegTestCase {

    private static String wellFormedId() {
        return new StreamId(new TaskId("some-node", randomLongBetween(1, 1_000_000)), randomAlphaOfLength(12)).encoded();
    }

    private static GetStreamResultRequest get(String id) {
        return new GetStreamResultRequest(id, 0, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT, null);
    }

    public void testGetIsNotFound() {
        String id = randomBoolean() ? wellFormedId() : randomFrom("*", "_all", "a,b", "not base64 !", randomAlphaOfLength(10));
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(EsqlStreamGetResultAction.INSTANCE, get(id)).actionGet()
        );
        assertThat(e.getMessage(), containsString(id));
    }

    public void testDeleteIsNotFound() {
        String id = randomBoolean() ? wellFormedId() : randomFrom("*", "_all", "a,b", "not base64 !", randomAlphaOfLength(10));
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(EsqlStreamDeleteAction.INSTANCE, new DeleteStreamResultRequest(id)).actionGet()
        );
        assertThat(e.getMessage(), containsString(id));
    }
}
