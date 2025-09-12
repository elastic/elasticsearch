/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.randomAlphanumericOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.safeGet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Utils for creating/retrieving/deleting ingest pipelines in a test cluster.
 */
public class IngestPipelineTestUtils {
    private static final Logger logger = LogManager.getLogger(IngestPipelineTestUtils.class);

    private IngestPipelineTestUtils() { /* no instances */ }

    /**
     * @param id         The pipeline id.
     * @param source     The body of the {@link PutPipelineRequest} as a JSON-formatted {@link BytesReference}.
     * @return a new {@link PutPipelineRequest} with the given {@code id} and body.
     */
    public static PutPipelineRequest putJsonPipelineRequest(String id, BytesReference source) {
        return new PutPipelineRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, id, source, XContentType.JSON);
    }

    /**
     * @param id         The pipeline id.
     * @param jsonString The body of the {@link PutPipelineRequest} as a JSON-formatted {@link String}.
     * @return a new {@link PutPipelineRequest} with the given {@code id} and body.
     */
    public static PutPipelineRequest putJsonPipelineRequest(String id, String jsonString) {
        return putJsonPipelineRequest(id, new BytesArray(jsonString));
    }

    /**
     * Create an ingest pipeline with the given ID and body, using the given {@link ElasticsearchClient}.
     *
     * @param client     The client to use to execute the {@link PutPipelineTransportAction}.
     * @param id         The pipeline id.
     * @param source     The body of the {@link PutPipelineRequest} as a JSON-formatted {@link BytesReference}.
     */
    public static void putJsonPipeline(ElasticsearchClient client, String id, BytesReference source) {
        assertAcked(safeGet(client.execute(PutPipelineTransportAction.TYPE, putJsonPipelineRequest(id, source))));
    }

    /**
     * Create an ingest pipeline with the given ID and body, using the given {@link ElasticsearchClient}.
     *
     * @param client     The client to use to execute the {@link PutPipelineTransportAction}.
     * @param id         The pipeline id.
     * @param jsonString The body of the {@link PutPipelineRequest} as a JSON-formatted {@link String}.
     */
    public static void putJsonPipeline(ElasticsearchClient client, String id, String jsonString) {
        putJsonPipeline(client, id, new BytesArray(jsonString));
    }

    /**
     * Create an ingest pipeline with the given ID and body, using the given {@link ElasticsearchClient}.
     *
     * @param client     The client to use to execute the {@link PutPipelineTransportAction}.
     * @param id         The pipeline id.
     * @param toXContent The body of the {@link PutPipelineRequest} as a {@link ToXContentFragment}.
     */
    public static void putJsonPipeline(ElasticsearchClient client, String id, ToXContentFragment toXContent) throws IOException {
        try (var xContentBuilder = jsonBuilder()) {
            xContentBuilder.startObject();
            toXContent.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            xContentBuilder.endObject();
            putJsonPipeline(client, id, BytesReference.bytes(xContentBuilder));
        }
    }

    /**
     * Attempt to delete the ingest pipeline with the given {@code id}, using the given {@link ElasticsearchClient}, and logging (but
     * otherwise ignoring) the result.
     */
    public static void deletePipelinesIgnoringExceptions(ElasticsearchClient client, Iterable<String> ids) {
        for (final var id : ids) {
            ESTestCase.safeAwait(
                l -> client.execute(
                    DeletePipelineTransportAction.TYPE,
                    new DeletePipelineRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, id),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            logger.info("delete pipeline [{}] success [acknowledged={}]", id, acknowledgedResponse.isAcknowledged());
                            l.onResponse(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(Strings.format("delete pipeline [%s] failure", id), e);
                            l.onResponse(null);
                        }
                    }
                )
            );
        }
    }

    /**
     * Construct a new {@link SimulatePipelineRequest} whose content is the given JSON document, represented as a {@link String}.
     */
    public static SimulatePipelineRequest jsonSimulatePipelineRequest(String jsonString) {
        return jsonSimulatePipelineRequest(new BytesArray(jsonString));
    }

    /**
     * Construct a new {@link SimulatePipelineRequest} whose content is the given JSON document, represented as a {@link BytesReference}.
     */
    public static SimulatePipelineRequest jsonSimulatePipelineRequest(BytesReference jsonBytes) {
        return new SimulatePipelineRequest(ReleasableBytesReference.wrap(jsonBytes), XContentType.JSON);
    }

    /**
     * Executes an action against an ingest document using a random access pattern. A synthetic pipeline instance with the provided
     * access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param document The document to operate on
     * @param action A consumer which takes the updated ingest document during execution
     * @throws Exception Any exception thrown from the provided consumer
     */
    public static void doWithRandomAccessPattern(IngestDocument document, Consumer<IngestDocument> action) throws Exception {
        doWithAccessPattern(randomFrom(IngestPipelineFieldAccessPattern.values()), document, action);
    }

    /**
     * Executes an action against an ingest document using a random access pattern. A synthetic pipeline instance with the provided
     * access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param accessPattern The access pattern to use when executing the block of code
     * @param document The document to operate on
     * @param action A consumer which takes the updated ingest document during execution
     * @throws Exception Any exception thrown from the provided consumer
     */
    public static void doWithAccessPattern(
        IngestPipelineFieldAccessPattern accessPattern,
        IngestDocument document,
        Consumer<IngestDocument> action
    ) throws Exception {
        runWithAccessPattern(accessPattern, document, new TestProcessor(action));
    }

    /**
     * Executes a processor against an ingest document using a random access pattern. A synthetic pipeline instance with the provided
     * access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param document The document to operate on
     * @param processor A processor which takes the updated ingest document during execution
     * @return the resulting ingest document instance
     * @throws Exception Any exception thrown from the provided consumer
     */
    public static IngestDocument runWithRandomAccessPattern(IngestDocument document, Processor processor) throws Exception {
        return runWithAccessPattern(randomFrom(IngestPipelineFieldAccessPattern.values()), document, processor);
    }

    /**
     * Executes a processor against an ingest document using the provided access pattern. A synthetic pipeline instance with the provided
     * access pattern is created and executed against the ingest document, thus updating its internal access pattern.
     * @param accessPattern The access pattern to use when executing the block of code
     * @param document The document to operate on
     * @param processor A processor which takes the updated ingest document during execution
     * @return the resulting ingest document instance
     * @throws Exception Any exception thrown from the provided consumer
     */
    public static IngestDocument runWithAccessPattern(
        IngestPipelineFieldAccessPattern accessPattern,
        IngestDocument document,
        Processor processor
    ) throws Exception {
        IngestDocument[] ingestDocumentHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        document.executePipeline(
            new Pipeline(
                randomAlphanumericOfLength(10),
                null,
                null,
                null,
                new CompoundProcessor(processor),
                accessPattern,
                null,
                null,
                null
            ),
            (result, ex) -> {
                ingestDocumentHolder[0] = result;
                exceptionHolder[0] = ex;
            }
        );
        Exception exception = exceptionHolder[0];
        if (exception != null) {
            if (exception instanceof IngestProcessorException ingestProcessorException) {
                exception = ((Exception) ingestProcessorException.getCause());
            }
            throw exception;
        }
        return ingestDocumentHolder[0];
    }
}
