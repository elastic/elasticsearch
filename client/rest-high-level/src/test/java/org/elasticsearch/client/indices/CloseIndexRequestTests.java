/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class CloseIndexRequestTests extends ESTestCase {

    public void testIndices() {
        String[] indices = generateRandomStringArray(5, 5, false, true);
        CloseIndexRequest request = new CloseIndexRequest(indices);
        assertArrayEquals(indices, request.indices());
    }

    public void testValidate() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        CloseIndexRequest request = new CloseIndexRequest(indices);
        Optional<ValidationException> validation = request.validate();
        assertTrue(validation.isPresent());
        assertThat(validation.get().validationErrors().get(0), equalTo("index is missing"));
    }

    public void testIndicesOptions() {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        CloseIndexRequest request = new CloseIndexRequest().indicesOptions(indicesOptions);
        assertEquals(indicesOptions, request.indicesOptions());
    }

    public void testWaitForActiveShards() {
        final CloseIndexRequest request = new CloseIndexRequest("index");
        final int count = randomIntBetween(0, 10);
        request.waitForActiveShards(ActiveShardCount.from(count));
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(count));
    }

    public void testTimeout() {
        final CloseIndexRequest request = new CloseIndexRequest("index");
        assertEquals(request.timeout(), TimedRequest.DEFAULT_ACK_TIMEOUT);
        assertEquals(request.masterNodeTimeout(), TimedRequest.DEFAULT_MASTER_NODE_TIMEOUT);

        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(0, 1000));
        request.setTimeout(timeout);

        final TimeValue masterTimeout = TimeValue.timeValueSeconds(randomIntBetween(0,1000));
        request.setMasterTimeout(masterTimeout);

        assertEquals(request.timeout(), timeout);
        assertEquals(request.masterNodeTimeout(), masterTimeout);
    }
}
