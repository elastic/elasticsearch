/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class FailureStoreDocumentConverterTests extends ESTestCase {

    public void testFailureStoreDocumentConverstion() throws Exception {
        IndexRequest source = new IndexRequest("original_index").routing("fake_routing")
            .id("1")
            .source(JsonXContent.contentBuilder().startObject().field("key", "value").endObject());

        // The exception will be wrapped for the test to make sure the converter correctly unwraps it
        Exception exception = new ElasticsearchException("Test exception please ignore");
        exception = new RemoteTransportException("Test exception wrapper, please ignore", exception);

        String targetIndexName = "rerouted_index";
        long testTime = 1702357200000L; // 2023-12-12T05:00:00.000Z

        IndexRequest convertedRequest = new FailureStoreDocumentConverter().transformFailedRequest(
            source,
            exception,
            targetIndexName,
            () -> testTime
        );

        // Retargeting write
        assertThat(convertedRequest.id(), is(nullValue()));
        assertThat(convertedRequest.routing(), is(nullValue()));
        assertThat(convertedRequest.index(), is(equalTo(targetIndexName)));
        assertThat(convertedRequest.opType(), is(DocWriteRequest.OpType.CREATE));

        // Original document content is no longer in same place
        assertThat("Expected original document to be modified", convertedRequest.sourceAsMap().get("key"), is(nullValue()));

        // Assert document contents
        assertThat(ObjectPath.eval("@timestamp", convertedRequest.sourceAsMap()), is(equalTo("2023-12-12T05:00:00.000Z")));

        assertThat(ObjectPath.eval("document.id", convertedRequest.sourceAsMap()), is(equalTo("1")));
        assertThat(ObjectPath.eval("document.routing", convertedRequest.sourceAsMap()), is(equalTo("fake_routing")));
        assertThat(ObjectPath.eval("document.index", convertedRequest.sourceAsMap()), is(equalTo("original_index")));
        assertThat(ObjectPath.eval("document.source.key", convertedRequest.sourceAsMap()), is(equalTo("value")));

        assertThat(ObjectPath.eval("error.type", convertedRequest.sourceAsMap()), is(equalTo("exception")));
        assertThat(ObjectPath.eval("error.message", convertedRequest.sourceAsMap()), is(equalTo("Test exception please ignore")));
        assertThat(
            ObjectPath.eval("error.stack_trace", convertedRequest.sourceAsMap()),
            startsWith("org.elasticsearch.ElasticsearchException: Test exception please ignore")
        );
        assertThat(
            ObjectPath.eval("error.stack_trace", convertedRequest.sourceAsMap()),
            containsString("at org.elasticsearch.action.bulk.FailureStoreDocumentTests.testFailureStoreDocumentConverstion")
        );

        assertThat(convertedRequest.isWriteToFailureStore(), is(true));
    }
}
