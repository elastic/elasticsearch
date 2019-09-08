/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointTests;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDoc;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStoredDocTests;
import org.elasticsearch.xpack.dataframe.DataFrameSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex.mappings;
import static org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager.TO_XCONTENT_PARAMS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DataFrameTransformsConfigManagerTests extends DataFrameSingleNodeTestCase {

    private DataFrameTransformsConfigManager transformsConfigManager;

    @Before
    public void createComponents() {
        transformsConfigManager = new DataFrameTransformsConfigManager(client(), xContentRegistry());
    }

    public void testGetMissingTransform() throws InterruptedException {
        // the index does not exist yet
        assertAsync(listener -> transformsConfigManager.getTransformConfiguration("not_there", listener), (DataFrameTransformConfig) null,
                null, e -> {
                    assertEquals(ResourceNotFoundException.class, e.getClass());
                    assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "not_there"),
                            e.getMessage());
                });

        // create one transform and test with an existing index
        assertAsync(
                listener -> transformsConfigManager
                        .putTransformConfiguration(DataFrameTransformConfigTests.randomDataFrameTransformConfig(), listener),
                true, null, null);

        // same test, but different code path
        assertAsync(listener -> transformsConfigManager.getTransformConfiguration("not_there", listener), (DataFrameTransformConfig) null,
                null, e -> {
                    assertEquals(ResourceNotFoundException.class, e.getClass());
                    assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "not_there"),
                            e.getMessage());
                });
    }

    public void testDeleteMissingTransform() throws InterruptedException {
        // the index does not exist yet
        assertAsync(listener -> transformsConfigManager.deleteTransform("not_there", listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "not_there"), e.getMessage());
        });

        // create one transform and test with an existing index
        assertAsync(
                listener -> transformsConfigManager
                        .putTransformConfiguration(DataFrameTransformConfigTests.randomDataFrameTransformConfig(), listener),
                true, null, null);

        // same test, but different code path
        assertAsync(listener -> transformsConfigManager.deleteTransform("not_there", listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "not_there"), e.getMessage());
        });
    }

    public void testCreateReadDeleteTransform() throws InterruptedException {
        DataFrameTransformConfig transformConfig = DataFrameTransformConfigTests.randomDataFrameTransformConfig();

        // create transform
        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig, listener), true, null, null);

        // read transform
        assertAsync(listener -> transformsConfigManager.getTransformConfiguration(transformConfig.getId(), listener), transformConfig, null,
                null);

        // try to create again
        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig, listener), (Boolean) null, null, e -> {
            assertEquals(ResourceAlreadyExistsException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, transformConfig.getId()),
                    e.getMessage());
        });

        // delete transform
        assertAsync(listener -> transformsConfigManager.deleteTransform(transformConfig.getId(), listener), true, null, null);

        // delete again
        assertAsync(listener -> transformsConfigManager.deleteTransform(transformConfig.getId(), listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformConfig.getId()),
                    e.getMessage());
        });

        // try to get deleted transform
        assertAsync(listener -> transformsConfigManager.getTransformConfiguration(transformConfig.getId(), listener),
                (DataFrameTransformConfig) null, null, e -> {
                    assertEquals(ResourceNotFoundException.class, e.getClass());
                    assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformConfig.getId()),
                            e.getMessage());
                });
    }

    public void testCreateReadDeleteCheckPoint() throws InterruptedException {
        DataFrameTransformCheckpoint checkpoint = DataFrameTransformCheckpointTests.randomDataFrameTransformCheckpoints();

        // create
        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        // read
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(checkpoint.getTransformId(), checkpoint.getCheckpoint(),
                listener), checkpoint, null, null);

        // delete
        assertAsync(listener -> transformsConfigManager.deleteTransform(checkpoint.getTransformId(), listener), true, null, null);

        // delete again
        assertAsync(listener -> transformsConfigManager.deleteTransform(checkpoint.getTransformId(), listener), (Boolean) null, null, e -> {
            assertEquals(ResourceNotFoundException.class, e.getClass());
            assertEquals(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, checkpoint.getTransformId()),
                    e.getMessage());
        });

        // getting a non-existing checkpoint returns null
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(checkpoint.getTransformId(), checkpoint.getCheckpoint(),
                listener), DataFrameTransformCheckpoint.EMPTY, null, null);
    }

    public void testExpandIds() throws Exception {
        DataFrameTransformConfig transformConfig1 = DataFrameTransformConfigTests.randomDataFrameTransformConfig("transform1_expand");
        DataFrameTransformConfig transformConfig2 = DataFrameTransformConfigTests.randomDataFrameTransformConfig("transform2_expand");
        DataFrameTransformConfig transformConfig3 = DataFrameTransformConfigTests.randomDataFrameTransformConfig("transform3_expand");

        // create transform
        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig1, listener), true, null, null);
        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig2, listener), true, null, null);
        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig3, listener), true, null, null);


        // expand 1 id
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds(transformConfig1.getId(),
                    PageParams.defaultParams(),
                    true,
                    listener),
            new Tuple<>(1L, Collections.singletonList("transform1_expand")),
            null,
            null);

        // expand 2 ids explicitly
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("transform1_expand,transform2_expand",
                    PageParams.defaultParams(),
                    true,
                    listener),
            new Tuple<>(2L, Arrays.asList("transform1_expand", "transform2_expand")),
            null,
            null);

        // expand 3 ids wildcard and explicit
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("transform1*,transform2_expand,transform3_expand",
                    PageParams.defaultParams(),
                    true,
                    listener),
            new Tuple<>(3L, Arrays.asList("transform1_expand", "transform2_expand", "transform3_expand")),
            null,
            null);

        // expand 3 ids _all
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("_all",
                    PageParams.defaultParams(),
                    true,
                    listener),
            new Tuple<>(3L, Arrays.asList("transform1_expand", "transform2_expand", "transform3_expand")),
            null,
            null);

        // expand 1 id _all with pagination
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("_all",
                    new PageParams(0, 1),
                    true,
                    listener),
            new Tuple<>(3L, Collections.singletonList("transform1_expand")),
            null,
            null);

        // expand 2 later ids _all with pagination
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("_all",
                    new PageParams(1, 2),
                    true,
                    listener),
            new Tuple<>(3L, Arrays.asList("transform2_expand", "transform3_expand")),
            null,
            null);

        // expand 1 id explicitly that does not exist
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("unknown,unknown2",
                    new PageParams(1, 2),
                    true,
                    listener),
            (Tuple<Long, List<String>>)null,
            null,
            e -> {
                assertThat(e, instanceOf(ResourceNotFoundException.class));
                assertThat(e.getMessage(),
                    equalTo(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "unknown,unknown2")));
            });

        // expand 1 id implicitly that does not exist
        assertAsync(listener ->
                transformsConfigManager.expandTransformIds("unknown*",
                    new PageParams(1, 2),
                    false,
                    listener),
            (Tuple<Long, List<String>>)null,
            null,
            e -> {
                assertThat(e, instanceOf(ResourceNotFoundException.class));
                assertThat(e.getMessage(),
                    equalTo(DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, "unknown*")));
            });

    }

    public void testStoredDoc() throws InterruptedException {
        String transformId = "transform_test_stored_doc_create_read_update";

        DataFrameTransformStoredDoc storedDocs = DataFrameTransformStoredDocTests.randomDataFrameTransformStoredDoc(transformId);
        SeqNoPrimaryTermAndIndex firstIndex = new SeqNoPrimaryTermAndIndex(0, 1, DataFrameInternalIndex.LATEST_INDEX_NAME);

        assertAsync(listener -> transformsConfigManager.putOrUpdateTransformStoredDoc(storedDocs, null, listener),
            firstIndex,
            null,
            null);
        assertAsync(listener -> transformsConfigManager.getTransformStoredDoc(transformId, listener),
            Tuple.tuple(storedDocs, firstIndex),
            null,
            null);

        SeqNoPrimaryTermAndIndex secondIndex = new SeqNoPrimaryTermAndIndex(1, 1, DataFrameInternalIndex.LATEST_INDEX_NAME);
        DataFrameTransformStoredDoc updated = DataFrameTransformStoredDocTests.randomDataFrameTransformStoredDoc(transformId);
        assertAsync(listener -> transformsConfigManager.putOrUpdateTransformStoredDoc(updated, firstIndex, listener),
            secondIndex,
            null,
            null);
        assertAsync(listener -> transformsConfigManager.getTransformStoredDoc(transformId, listener),
            Tuple.tuple(updated, secondIndex),
            null,
            null);

        assertAsync(listener -> transformsConfigManager.putOrUpdateTransformStoredDoc(updated, firstIndex, listener),
            (SeqNoPrimaryTermAndIndex)null,
            r -> fail("did not fail with version conflict."),
            e -> assertThat(
                e.getMessage(),
                equalTo("Failed to persist data frame statistics for transform [transform_test_stored_doc_create_read_update]"))
            );
    }

    public void testGetStoredDocMultiple() throws InterruptedException {
        int numStats = randomIntBetween(10, 15);
        List<DataFrameTransformStoredDoc> expectedDocs = new ArrayList<>();
        for (int i=0; i<numStats; i++) {
            SeqNoPrimaryTermAndIndex initialSeqNo =
                new SeqNoPrimaryTermAndIndex(i, 1, DataFrameInternalIndex.LATEST_INDEX_NAME);
            DataFrameTransformStoredDoc stat =
                    DataFrameTransformStoredDocTests.randomDataFrameTransformStoredDoc(randomAlphaOfLength(6) + i);
            expectedDocs.add(stat);
            assertAsync(listener -> transformsConfigManager.putOrUpdateTransformStoredDoc(stat, null, listener),
                initialSeqNo,
                null,
                null);
        }

        // remove one of the put docs so we don't retrieve all
        if (expectedDocs.size() > 1) {
            expectedDocs.remove(expectedDocs.size() - 1);
        }
        List<String> ids = expectedDocs.stream().map(DataFrameTransformStoredDoc::getId).collect(Collectors.toList());

        // returned docs will be ordered by id
        expectedDocs.sort(Comparator.comparing(DataFrameTransformStoredDoc::getId));
        assertAsync(listener -> transformsConfigManager.getTransformStoredDoc(ids, listener), expectedDocs, null, null);
    }

    public void testDeleteOldTransformConfigurations() throws Exception {
        String oldIndex = DataFrameInternalIndex.INDEX_PATTERN + "1";
        String transformId = "transform_test_delete_old_configurations";
        String docId = DataFrameTransformConfig.documentId(transformId);
        DataFrameTransformConfig transformConfig = DataFrameTransformConfigTests
            .randomDataFrameTransformConfig("transform_test_delete_old_configurations");
        client().admin().indices().create(new CreateIndexRequest(oldIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappings())).actionGet();

        try(XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = transformConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest request = new IndexRequest(oldIndex)
                .source(source)
                .id(docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(request).actionGet();
        }

        assertAsync(listener -> transformsConfigManager.putTransformConfiguration(transformConfig, listener), true, null, null);

        assertThat(client().get(new GetRequest(oldIndex).id(docId)).actionGet().isExists(), is(true));
        assertThat(client().get(new GetRequest(DataFrameInternalIndex.LATEST_INDEX_NAME).id(docId)).actionGet().isExists(), is(true));

        assertAsync(listener -> transformsConfigManager.deleteOldTransformConfigurations(transformId, listener), true, null, null);

        client().admin().indices().refresh(new RefreshRequest(DataFrameInternalIndex.INDEX_NAME_PATTERN)).actionGet();
        assertThat(client().get(new GetRequest(oldIndex).id(docId)).actionGet().isExists(), is(false));
        assertThat(client().get(new GetRequest(DataFrameInternalIndex.LATEST_INDEX_NAME).id(docId)).actionGet().isExists(), is(true));
    }

    public void testDeleteOldTransformStoredDocuments() throws Exception {
        String oldIndex = DataFrameInternalIndex.INDEX_PATTERN + "1";
        String transformId = "transform_test_delete_old_stored_documents";
        String docId = DataFrameTransformStoredDoc.documentId(transformId);
        DataFrameTransformStoredDoc dataFrameTransformStoredDoc = DataFrameTransformStoredDocTests
            .randomDataFrameTransformStoredDoc(transformId);
        client().admin().indices().create(new CreateIndexRequest(oldIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappings())).actionGet();

        try(XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = dataFrameTransformStoredDoc.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest request = new IndexRequest(oldIndex)
                .source(source)
                .id(docId);
            client().index(request).actionGet();
        }

        // Put when referencing the old index should create the doc in the new index, even if we have seqNo|primaryTerm info
        assertAsync(listener -> transformsConfigManager.putOrUpdateTransformStoredDoc(dataFrameTransformStoredDoc,
            new SeqNoPrimaryTermAndIndex(3, 1, oldIndex),
            listener),
            new SeqNoPrimaryTermAndIndex(0, 1, DataFrameInternalIndex.LATEST_INDEX_NAME),
            null,
            null);

        client().admin().indices().refresh(new RefreshRequest(DataFrameInternalIndex.INDEX_NAME_PATTERN)).actionGet();

        assertThat(client().get(new GetRequest(oldIndex).id(docId)).actionGet().isExists(), is(true));
        assertThat(client().get(new GetRequest(DataFrameInternalIndex.LATEST_INDEX_NAME).id(docId)).actionGet().isExists(), is(true));

        assertAsync(listener -> transformsConfigManager.deleteOldTransformStoredDocuments(transformId, listener),
            true,
            null,
            null);

        client().admin().indices().refresh(new RefreshRequest(DataFrameInternalIndex.INDEX_NAME_PATTERN)).actionGet();
        assertThat(client().get(new GetRequest(oldIndex).id(docId)).actionGet().isExists(), is(false));
        assertThat(client().get(new GetRequest(DataFrameInternalIndex.LATEST_INDEX_NAME).id(docId)).actionGet().isExists(), is(true));
    }
}
