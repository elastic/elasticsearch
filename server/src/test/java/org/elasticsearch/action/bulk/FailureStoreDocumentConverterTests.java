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
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class FailureStoreDocumentConverterTests extends ESTestCase {

    public void testFailureStoreDocumentConversion() throws Exception {
        IndexRequest source = new IndexRequest("original_index").routing("fake_routing")
            .id("1")
            .source(JsonXContent.contentBuilder().startObject().field("key", "value").endObject());

        // The exception will be wrapped for the test to make sure the converter correctly unwraps it
        ElasticsearchException exception = new ElasticsearchException("Test exception please ignore");
        ElasticsearchException ingestException = exception;
        if (randomBoolean()) {
            ingestException = new ElasticsearchException("Test suppressed exception, please ignore");
            exception.addSuppressed(ingestException);
        }
        boolean withPipelineOrigin = randomBoolean();
        if (withPipelineOrigin) {
            ingestException.addHeader(
                CompoundProcessor.PIPELINE_ORIGIN_EXCEPTION_HEADER,
                Arrays.asList("some-failing-pipeline", "some-pipeline")
            );
        }
        boolean withProcessorTag = randomBoolean();
        if (withProcessorTag) {
            ingestException.addHeader(CompoundProcessor.PROCESSOR_TAG_EXCEPTION_HEADER, "foo-tag");
        }
        boolean withProcessorType = randomBoolean();
        if (withProcessorType) {
            ingestException.addHeader(CompoundProcessor.PROCESSOR_TYPE_EXCEPTION_HEADER, "bar-type");
        }
        if (randomBoolean()) {
            exception = new RemoteTransportException("Test exception wrapper, please ignore", exception);
        }

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
            startsWith("o.e.ElasticsearchException: Test exception please ignore")
        );
        assertThat(
            ObjectPath.eval("error.stack_trace", convertedRequest.sourceAsMap()),
            containsString("at o.e.a.b.FailureStoreDocumentConverterTests.testFailureStoreDocumentConversion")
        );
        assertThat(
            ObjectPath.eval("error.pipeline_trace", convertedRequest.sourceAsMap()),
            is(equalTo(withPipelineOrigin ? List.of("some-pipeline", "some-failing-pipeline") : null))
        );
        assertThat(
            ObjectPath.eval("error.pipeline", convertedRequest.sourceAsMap()),
            is(equalTo(withPipelineOrigin ? "some-failing-pipeline" : null))
        );
        assertThat(
            ObjectPath.eval("error.processor_tag", convertedRequest.sourceAsMap()),
            is(equalTo(withProcessorTag ? "foo-tag" : null))
        );
        assertThat(
            ObjectPath.eval("error.processor_type", convertedRequest.sourceAsMap()),
            is(equalTo(withProcessorType ? "bar-type" : null))
        );

        assertThat(convertedRequest.isWriteToFailureStore(), is(true));
    }
}
