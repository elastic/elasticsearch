/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.put;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchRequest;

import static org.hamcrest.Matchers.is;

public class PutWatchSerializationTests extends ESTestCase {

    // https://github.com/elastic/x-plugins/issues/2490
    public void testPutWatchSerialization() throws Exception {
        PutWatchRequest request = new PutWatchRequest();
        request.setId(randomAsciiOfLength(10));
        request.setActive(randomBoolean());
        request.setSource(new BytesArray(randomAsciiOfLength(20)));

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);

        PutWatchRequest readRequest = new PutWatchRequest();
        readRequest.readFrom(streamOutput.bytes().streamInput());
        assertThat(readRequest.isActive(), is(request.isActive()));
        assertThat(readRequest.getId(), is(request.getId()));
        assertThat(readRequest.getSource(), is(request.getSource()));
    }

}
