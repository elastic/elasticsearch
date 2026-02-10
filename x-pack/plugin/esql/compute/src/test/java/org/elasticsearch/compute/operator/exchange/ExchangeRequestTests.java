/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;

public class ExchangeRequestTests extends ESTestCase {

    public void testParentTask() {
        ExchangeRequest r1 = new ExchangeRequest("1", true, new String[] { "test-index" }, IndicesOptions.STRICT_EXPAND_OPEN);
        r1.setParentTask(new TaskId("node-1", 1));
        assertSame(TaskId.EMPTY_TASK_ID, r1.getParentTask());

        ExchangeRequest r2 = new ExchangeRequest("1", false, new String[] { "test-index" }, IndicesOptions.STRICT_EXPAND_OPEN);
        r2.setParentTask(new TaskId("node-2", 2));
        assertTrue(r2.getParentTask().isSet());
        assertThat(r2.getParentTask(), equalTo((new TaskId("node-2", 2))));
    }

    public void testSerializationWithIndicesContext() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT);
        String[] indices = new String[] { "index-a", "index-b" };
        ExchangeRequest original = new ExchangeRequest("session-1", false, indices, IndicesOptions.STRICT_EXPAND_OPEN);

        ExchangeRequest deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            ExchangeRequest::new,
            version
        );

        assertThat(deserialized.exchangeId(), equalTo("session-1"));
        assertFalse(deserialized.sourcesFinished());
        assertThat(deserialized.indices(), arrayContaining("index-a", "index-b"));
        assertThat(deserialized.indicesOptions(), equalTo(IndicesOptions.STRICT_EXPAND_OPEN));
    }

    public void testSerializationBeforeIndicesContext() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT);
        String[] indices = new String[] { "index-a", "index-b" };
        ExchangeRequest original = new ExchangeRequest("session-2", true, indices, IndicesOptions.lenientExpandOpen());

        ExchangeRequest deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            ExchangeRequest::new,
            version
        );

        assertThat(deserialized.exchangeId(), equalTo("session-2"));
        assertTrue(deserialized.sourcesFinished());
        // Indices are not serialized on older transport versions; deserialization falls back to empty
        assertThat(deserialized.indices(), emptyArray());
        // IndicesOptions fall back to STRICT_EXPAND_OPEN on older transport versions
        assertThat(deserialized.indicesOptions(), equalTo(IndicesOptions.STRICT_EXPAND_OPEN));
    }

    public void testSerializationWithEmptyIndices() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT);
        ExchangeRequest original = new ExchangeRequest("session-3", false, new String[0], IndicesOptions.STRICT_EXPAND_OPEN);

        ExchangeRequest deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            ExchangeRequest::new,
            version
        );

        assertThat(deserialized.exchangeId(), equalTo("session-3"));
        assertThat(deserialized.indices(), emptyArray());
        assertThat(deserialized.indicesOptions(), equalTo(IndicesOptions.STRICT_EXPAND_OPEN));
    }

    public void testSerializationAtCurrentVersion() throws IOException {
        String[] indices = new String[] { "my-index" };
        ExchangeRequest original = new ExchangeRequest("session-4", false, indices, IndicesOptions.STRICT_EXPAND_OPEN);

        ExchangeRequest deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            ExchangeRequest::new,
            TransportVersion.current()
        );

        assertThat(deserialized.exchangeId(), equalTo("session-4"));
        assertFalse(deserialized.sourcesFinished());
        assertThat(deserialized.indices(), arrayContaining("my-index"));
        assertThat(deserialized.indicesOptions(), equalTo(IndicesOptions.STRICT_EXPAND_OPEN));
    }
}
