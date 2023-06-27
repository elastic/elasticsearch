/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.ChunkedTrainedModelRestorer;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasSize;

public class ChunkedTrainedModelRestorerIT extends MlSingleNodeTestCase {

    public void testRestoreWithMultipleSearches() throws IOException, InterruptedException {
        String modelId = "test-multiple-searches";
        int numDocs = 22;
        List<BytesReference> modelDefs = new ArrayList<>(numDocs);

        for (int i = 0; i < numDocs; i++) {
            // actual content of the model definition is not important here
            modelDefs.add(new BytesArray(Base64.getEncoder().encode(("model_def_" + i).getBytes(StandardCharsets.UTF_8))));
        }

        List<TrainedModelDefinitionDoc> expectedDocs = createModelDefinitionDocs(modelDefs, modelId);
        putModelDefinitions(expectedDocs, InferenceIndexConstants.LATEST_INDEX_NAME, 0);

        ChunkedTrainedModelRestorer restorer = new ChunkedTrainedModelRestorer(
            modelId,
            client(),
            client().threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            xContentRegistry()
        );
        restorer.setSearchSize(5);
        List<TrainedModelDefinitionDoc> actualDocs = new ArrayList<>();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        restorer.restoreModelDefinition(actualDocs::add, success -> latch.countDown(), failure -> {
            exceptionHolder.set(failure);
            latch.countDown();
        });

        latch.await();

        assertNull(exceptionHolder.get());
        assertEquals(actualDocs, expectedDocs);
    }

    public void testCancel() throws IOException, InterruptedException {
        String modelId = "test-cancel-search";
        int numDocs = 6;
        List<BytesReference> modelDefs = new ArrayList<>(numDocs);

        for (int i = 0; i < numDocs; i++) {
            // actual content of the model definition is not important here
            modelDefs.add(new BytesArray(Base64.getEncoder().encode(("model_def_" + i).getBytes(StandardCharsets.UTF_8))));
        }

        List<TrainedModelDefinitionDoc> expectedDocs = createModelDefinitionDocs(modelDefs, modelId);
        putModelDefinitions(expectedDocs, InferenceIndexConstants.LATEST_INDEX_NAME, 0);

        ChunkedTrainedModelRestorer restorer = new ChunkedTrainedModelRestorer(
            modelId,
            client(),
            client().threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            xContentRegistry()
        );
        restorer.setSearchSize(5);
        List<TrainedModelDefinitionDoc> actualDocs = new ArrayList<>();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicBoolean successValue = new AtomicBoolean(Boolean.TRUE);
        CountDownLatch latch = new CountDownLatch(1);

        restorer.restoreModelDefinition(doc -> {
            actualDocs.add(doc);
            return false;
        }, success -> {
            successValue.set(success);
            latch.countDown();
        }, failure -> {
            exceptionHolder.set(failure);
            latch.countDown();
        });

        latch.await();

        assertNull(exceptionHolder.get());
        assertFalse(successValue.get());
        assertThat(actualDocs, hasSize(1));
        assertEquals(expectedDocs.get(0), actualDocs.get(0));
    }

    public void testRestoreWithDocumentsInMultipleIndices() throws IOException, InterruptedException {
        String index1 = "foo-1";
        String index2 = "foo-2";

        for (String index : new String[] { index1, index2 }) {
            indicesAdmin().prepareCreate(index)
                .setMapping(
                    TrainedModelDefinitionDoc.DEFINITION.getPreferredName(),
                    "type=binary",
                    InferenceIndexConstants.DOC_TYPE.getPreferredName(),
                    "type=keyword",
                    TrainedModelConfig.MODEL_ID.getPreferredName(),
                    "type=keyword"
                )
                .get();
        }

        String modelId = "test-multiple-indices";
        int numDocs = 24;
        List<BytesReference> modelDefs = new ArrayList<>(numDocs);

        for (int i = 0; i < numDocs; i++) {
            // actual content of the model definition is not important here
            modelDefs.add(new BytesArray(Base64.getEncoder().encode(("model_def_" + i).getBytes(StandardCharsets.UTF_8))));
        }

        List<TrainedModelDefinitionDoc> expectedDocs = createModelDefinitionDocs(modelDefs, modelId);
        int splitPoint = (numDocs / 2) - 1;
        putModelDefinitions(expectedDocs.subList(0, splitPoint), index1, 0);
        putModelDefinitions(expectedDocs.subList(splitPoint, numDocs), index2, splitPoint);

        ChunkedTrainedModelRestorer restorer = new ChunkedTrainedModelRestorer(
            modelId,
            client(),
            client().threadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            xContentRegistry()
        );
        restorer.setSearchSize(10);
        restorer.setSearchIndex("foo-*");

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<TrainedModelDefinitionDoc> actualDocs = new ArrayList<>();

        restorer.restoreModelDefinition(actualDocs::add, success -> latch.countDown(), failure -> {
            exceptionHolder.set(failure);
            latch.countDown();
        });

        latch.await();

        assertNull(exceptionHolder.get());
        // The results are sorted by index first rather than doc_num
        // TODO is this the behaviour we want?
        List<TrainedModelDefinitionDoc> reorderedDocs = new ArrayList<>();
        reorderedDocs.addAll(expectedDocs.subList(splitPoint, numDocs));
        reorderedDocs.addAll(expectedDocs.subList(0, splitPoint));
        assertEquals(actualDocs, reorderedDocs);
    }

    private List<TrainedModelDefinitionDoc> createModelDefinitionDocs(List<BytesReference> compressedDefinitions, String modelId) {
        int totalLength = compressedDefinitions.stream().map(BytesReference::length).reduce(0, Integer::sum);

        List<TrainedModelDefinitionDoc> docs = new ArrayList<>();
        for (int i = 0; i < compressedDefinitions.size(); i++) {
            docs.add(
                new TrainedModelDefinitionDoc.Builder().setDocNum(i)
                    .setBinaryData(compressedDefinitions.get(i))
                    .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                    .setTotalDefinitionLength(totalLength)
                    .setDefinitionLength(compressedDefinitions.get(i).length())
                    .setEos(i == compressedDefinitions.size() - 1)
                    .setModelId(modelId)
                    .build()
            );
        }

        return docs;
    }

    private void putModelDefinitions(List<TrainedModelDefinitionDoc> docs, String index, int startingDocNum) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (TrainedModelDefinitionDoc doc : docs) {
            try (XContentBuilder xContentBuilder = doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
                IndexRequestBuilder indexRequestBuilder = client().prepareIndex(index)
                    .setSource(xContentBuilder)
                    .setId(TrainedModelDefinitionDoc.docId(doc.getModelId(), startingDocNum++));

                bulkRequestBuilder.add(indexRequestBuilder);
            }
        }

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (bulkResponse.hasFailures()) {
            int failures = 0;
            for (BulkItemResponse itemResponse : bulkResponse) {
                if (itemResponse.isFailed()) {
                    failures++;
                    logger.error("Item response failure [{}]", itemResponse.getFailureMessage());
                }
            }
            fail("Bulk response contained " + failures + " failures");
        }
    }
}
