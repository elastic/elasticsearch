/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import joptsimple.internal.Strings;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

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

    public void testBasicMigration() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        indicesAdmin().create(new CreateIndexRequest("index1")).get();
        indicesAdmin().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(indicesAdmin().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        client().execute(
            MigrateToDataStreamAction.INSTANCE,
            new MigrateToDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, alias)
        ).get();

        resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
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
        indicesAdmin().create(new CreateIndexRequest("index1")).get();
        indicesAdmin().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(indicesAdmin().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(
                MigrateToDataStreamAction.INSTANCE,
                new MigrateToDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, alias)
            ).get()
        );

        assertTrue(
            throwableOrItsCause(e, IllegalArgumentException.class, "no matching index template found for data stream [" + alias + "]")
        );
    }

    public void testMigrationWithoutIndexMappings() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        indicesAdmin().create(new CreateIndexRequest("index1")).get();
        indicesAdmin().create(new CreateIndexRequest("index2")).get();

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(indicesAdmin().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(
                MigrateToDataStreamAction.INSTANCE,
                new MigrateToDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, alias)
            ).get()
        );

        assertTrue(throwableOrItsCause(e, IllegalArgumentException.class, "must have mappings for a timestamp field"));
    }

    public void testMigrationWithoutTimestampMapping() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        indicesAdmin().create(new CreateIndexRequest("index1")).get();
        indicesAdmin().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1, "foo");
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2, "foo");

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(indicesAdmin().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(
                MigrateToDataStreamAction.INSTANCE,
                new MigrateToDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, alias)
            ).get()
        );

        assertTrue(throwableOrItsCause(e, IllegalArgumentException.class, "data stream timestamp field [@timestamp] does not exist"));
    }

    public void testMigrationWithoutWriteIndex() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        indicesAdmin().create(new CreateIndexRequest("index1")).get();
        indicesAdmin().create(new CreateIndexRequest("index2")).get();

        int numDocs1 = randomIntBetween(2, 16);
        indexDocs("index1", numDocs1);
        int numDocs2 = randomIntBetween(2, 16);
        indexDocs("index2", numDocs2);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(false));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(indicesAdmin().aliases(request).get());

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            new String[] { "*" },
            IndicesOptions.fromOptions(true, true, true, true, true)
        );
        ResolveIndexAction.Response resolveResponse = indicesAdmin().resolveIndex(resolveRequest).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getAliases().get(0).getName(), equalTo(alias));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        assertThat(resolveResponse.getIndices().size(), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(
                MigrateToDataStreamAction.INSTANCE,
                new MigrateToDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, alias)
            ).get()
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
        indicesAdmin().refresh(new RefreshRequest(index)).actionGet();
    }

}
