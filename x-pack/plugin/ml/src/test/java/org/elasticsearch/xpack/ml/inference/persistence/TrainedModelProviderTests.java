/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;

public class TrainedModelProviderTests extends ESTestCase {

    public void testDeleteModelStoredAsResource() {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        // Should be OK as we don't make any client calls
        trainedModelProvider.deleteTrainedModel("lang_ident_model_1", future);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(
            Messages.INFERENCE_CANNOT_DELETE_ML_MANAGED_MODEL,
            "lang_ident_model_1"
        )));
    }

    public void testPutModelThatExistsAsResource() {
        TrainedModelConfig config = TrainedModelConfigTests.createTestInstance("lang_ident_model_1").build();
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        trainedModelProvider.storeTrainedModel(config, future);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, "lang_ident_model_1")));
    }

    public void testGetModelThatExistsAsResource() throws Exception {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        for(String modelId : TrainedModelProvider.MODELS_STORED_AS_RESOURCE) {
            PlainActionFuture<TrainedModelConfig> future = new PlainActionFuture<>();
            trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(),future);
            TrainedModelConfig configWithDefinition = future.actionGet();

            assertThat(configWithDefinition.getModelId(), equalTo(modelId));
            assertThat(configWithDefinition.ensureParsedDefinition(xContentRegistry()).getModelDefinition(), is(not(nullValue())));

            PlainActionFuture<TrainedModelConfig> futureNoDefinition = new PlainActionFuture<>();
            trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.empty(), futureNoDefinition);
            TrainedModelConfig configWithoutDefinition = futureNoDefinition.actionGet();

            assertThat(configWithoutDefinition.getModelId(), equalTo(modelId));
            assertThat(configWithDefinition.ensureParsedDefinition(xContentRegistry()).getModelDefinition(), is(not(nullValue())));
        }
    }

    public void testExpandIdsQuery() {
        QueryBuilder queryBuilder = TrainedModelProvider.buildExpandIdsQuery(new String[]{"model*", "trained_mode"},
            Arrays.asList("tag1", "tag2"));
        assertThat(queryBuilder, is(instanceOf(ConstantScoreQueryBuilder.class)));

        QueryBuilder innerQuery = ((ConstantScoreQueryBuilder)queryBuilder).innerQuery();
        assertThat(innerQuery, is(instanceOf(BoolQueryBuilder.class)));

        ((BoolQueryBuilder)innerQuery).filter().forEach(qb -> {
            if (qb instanceof TermQueryBuilder) {
                assertThat(((TermQueryBuilder)qb).fieldName(), equalTo(TrainedModelConfig.TAGS.getPreferredName()));
                assertThat(((TermQueryBuilder)qb).value(), is(oneOf("tag1", "tag2")));
                return;
            }
            assertThat(qb, is(instanceOf(BoolQueryBuilder.class)));
        });
    }

    public void testExpandIdsPagination() {
        // NOTE: these tests assume that the query pagination results are "buffered"

        assertThat(TrainedModelProvider.collectIds(new PageParams(0, 3),
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("a", "b", "c"))),
            equalTo(new TreeSet<>(Arrays.asList("a", "b", "c"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(0, 3),
            Collections.singleton("a"),
            new HashSet<>(Arrays.asList("b", "c", "d"))),
            equalTo(new TreeSet<>(Arrays.asList("a", "b", "c"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(1, 3),
            Collections.singleton("a"),
            new HashSet<>(Arrays.asList("b", "c", "d"))),
            equalTo(new TreeSet<>(Arrays.asList("b", "c", "d"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(1, 1),
            Collections.singleton("c"),
            new HashSet<>(Arrays.asList("a", "b"))),
            equalTo(new TreeSet<>(Arrays.asList("b"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(1, 1),
            Collections.singleton("b"),
            new HashSet<>(Arrays.asList("a", "c"))),
            equalTo(new TreeSet<>(Arrays.asList("b"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(1, 2),
            new HashSet<>(Arrays.asList("a", "b")),
            new HashSet<>(Arrays.asList("c", "d", "e"))),
            equalTo(new TreeSet<>(Arrays.asList("b", "c"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(1, 3),
            new HashSet<>(Arrays.asList("a", "b")),
            new HashSet<>(Arrays.asList("c", "d", "e"))),
            equalTo(new TreeSet<>(Arrays.asList("b", "c", "d"))));

        assertThat(TrainedModelProvider.collectIds(new PageParams(2, 3),
            new HashSet<>(Arrays.asList("a", "b")),
            new HashSet<>(Arrays.asList("c", "d", "e"))),
            equalTo(new TreeSet<>(Arrays.asList("c", "d", "e"))));
    }

    public void testGetModelThatExistsAsResourceButIsMissing() {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> trainedModelProvider.loadModelFromResource("missing_model", randomBoolean()));
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, "missing_model")));
    }

    public void testChunkDefinitionWithSize() {
        int totalLength = 100;
        int size = 30;

        byte[] bytes = randomByteArrayOfLength(totalLength);
        List<BytesReference> chunks = TrainedModelProvider.chunkDefinitionWithSize(new BytesArray(bytes), size);
        assertThat(chunks, hasSize(4));
        int start = 0;
        int end = size;
        for (BytesReference chunk : chunks) {
            assertArrayEquals(Arrays.copyOfRange(bytes, start, end),
                Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + chunk.length()));

            start += size;
            end = Math.min(end + size, totalLength);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
