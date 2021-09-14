/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.datastreams;

import joptsimple.internal.Strings;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.MigrateToDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.DataStreamIT.putComposableIndexTemplate;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamMigrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @After
    public void cleanup() {
        AcknowledgedResponse response = client().execute(
            DeleteDataStreamAction.INSTANCE,
            new DeleteDataStreamAction.Request(new String[] { "*" })
        ).actionGet();
        assertAcked(response);

        DeleteDataStreamAction.Request deleteDSRequest = new DeleteDataStreamAction.Request(new String[] { "*" });
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDSRequest).actionGet();
        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet();
    }

    public void testBasicMigration() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get();

        resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(0));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(1));
        assertThat(resolveResponse.getDataStreams().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().get(0).getBackingIndices(), arrayContaining("index2", "index1"));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        int numDocsDs = randomIntBetween(2, 16);
        indexDocs(alias, numDocsDs);
        DataStreamIT.verifyDocs(alias, numDocs1 + numDocs2 + numDocsDs, List.of("index1", "index2"));
    }

    public void testMigrationWithoutTemplate() throws Exception {
        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get()
        );

        assertTrue(
            throwableOrItsCause(e, IllegalArgumentException.class, "no matching index template found for data stream [" + alias + "]")
        );
    }

    public void testMigrationWithoutIndexMappings() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get()
        );

        assertTrue(throwableOrItsCause(e, IllegalArgumentException.class, "must have mappings for a timestamp field"));
    }

    public void testMigrationWithoutTimestampMapping() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1, "foo");
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2, "foo");

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get()
        );

        assertTrue(throwableOrItsCause(e, IllegalArgumentException.class, "data stream timestamp field [@timestamp] does not exist"));
    }

    public void testMigrationWithoutWriteIndex() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(false));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = admin().indices().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get()
        );

        assertTrue(throwableOrItsCause(e, IllegalArgumentException.class, "alias [" + alias + "] must specify a write index"));
    }

    static <T> boolean throwableOrItsCause(Throwable t, Class<T> clazz, String message) {
        boolean found = false;
        Throwable throwable = t;
        while (throwable != null && found == false) {
            found = throwable.getMessage().contains(message) && throwable.getClass().equals(clazz);
            throwable = throwable.getCause();
        }
        return found;
    }

    static void indexDocs(String index, int numDocs) {
        indexDocs(index, numDocs, Strings.EMPTY);
    }

    static void indexDocs(String index, int numDocs, String fieldPrefix) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", fieldPrefix + DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
        }
        client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

}
