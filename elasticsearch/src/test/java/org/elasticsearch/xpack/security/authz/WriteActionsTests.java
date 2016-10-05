/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthorizationException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class WriteActionsTests extends SecurityIntegTestCase {

    @Override
    protected String configRoles() {
        return SecuritySettingsSource.DEFAULT_ROLE + ":\n" +
                "  cluster: [ ALL ]\n" +
                "  indices:\n" +
                "    - names: 'missing'\n" +
                "      privileges: [ 'indices:admin/create', 'indices:admin/delete' ]\n" +
                "    - names: ['/index.*/']\n" +
                "      privileges: [ manage ]\n" +
                "    - names: ['/test.*/']\n" +
                "      privileges: [ manage, write ]\n" +
                "    - names: '/test.*/'\n" +
                "      privileges: [ read ]\n";
    }

    public void testIndex() {
        createIndex("test1", "index1");
        client().prepareIndex("test1", "type", "id").setSource("field", "value").get();

        assertThrowsAuthorizationException(client().prepareIndex("index1", "type", "id").setSource("field", "value"));

        client().prepareIndex("test4", "type", "id").setSource("field", "value").get();
        //the missing index gets automatically created (user has permissions for that), but indexing fails due to missing authorization
        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareIndex("missing", "type", "id").setSource("field", "value").get());
        assertAuthorizationException(exception);
        assertThat(exception.getMessage(), containsString("[indices:data/write/index] is unauthorized"));
    }

    public void testDelete() {
        createIndex("test1", "index1");
        client().prepareIndex("test1", "type", "id").setSource("field", "value").get();
        assertEquals(RestStatus.OK, client().prepareDelete("test1", "type", "id").get().status());

        assertThrowsAuthorizationException(client().prepareDelete("index1", "type", "id"));

        assertEquals(RestStatus.NOT_FOUND, client().prepareDelete("test4", "type", "id").get().status());

        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareDelete("missing", "type", "id").get());
        assertAuthorizationException(exception);
        assertThat(exception.getMessage(), containsString("[indices:data/write/delete] is unauthorized"));
    }

    public void testUpdate() {
        createIndex("test1", "index1");
        client().prepareIndex("test1", "type", "id").setSource("field", "value").get();
        assertEquals(RestStatus.OK, client().prepareUpdate("test1", "type", "id").setDoc("field2", "value2").get().status());

        assertThrowsAuthorizationException(client().prepareUpdate("index1", "type", "id").setDoc("field2", "value2"));

        expectThrows(DocumentMissingException.class, () -> client().prepareUpdate("test4", "type", "id").setDoc("field2", "value2").get());

        ElasticsearchSecurityException exception = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareUpdate("missing", "type", "id").setDoc("field2", "value2").get());
        assertAuthorizationException(exception);
        assertThat(exception.getMessage(), containsString("[indices:data/write/update] is unauthorized"));
    }

    public void testBulk() {
        createIndex("test1", "test2", "test3", "index1");
        BulkResponse bulkResponse = client().prepareBulk()
                .add(new IndexRequest("test1", "type", "id").source("field", "value"))
                .add(new IndexRequest("index1", "type", "id").source("field", "value"))
                .add(new IndexRequest("test4", "type", "id").source("field", "value"))
                .add(new IndexRequest("missing", "type", "id").source("field", "value"))
                .add(new DeleteRequest("test1", "type", "id"))
                .add(new DeleteRequest("index1", "type", "id"))
                .add(new DeleteRequest("test4", "type", "id"))
                .add(new DeleteRequest("missing", "type", "id"))
                .add(new IndexRequest("test1", "type", "id").source("field", "value"))
                .add(new UpdateRequest("test1", "type", "id").doc("field", "value"))
                .add(new UpdateRequest("index1", "type", "id").doc("field", "value"))
                .add(new UpdateRequest("test4", "type", "id").doc("field", "value"))
                .add(new UpdateRequest("missing", "type", "id").doc("field", "value")).get();
        assertTrue(bulkResponse.hasFailures());
        assertEquals(13, bulkResponse.getItems().length);
        assertFalse(bulkResponse.getItems()[0].isFailed());
        assertEquals(DocWriteRequest.OpType.INDEX, bulkResponse.getItems()[0].getOpType());
        assertEquals("test1", bulkResponse.getItems()[0].getIndex());
        assertTrue(bulkResponse.getItems()[1].isFailed());
        assertEquals(DocWriteRequest.OpType.INDEX, bulkResponse.getItems()[1].getOpType());
        assertEquals("index1", bulkResponse.getItems()[1].getFailure().getIndex());
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[1].getFailure().getCause());
        assertThat(bulkResponse.getItems()[1].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
        assertFalse(bulkResponse.getItems()[2].isFailed());
        assertEquals(DocWriteRequest.OpType.INDEX, bulkResponse.getItems()[2].getOpType());
        assertEquals("test4", bulkResponse.getItems()[2].getResponse().getIndex());
        assertTrue(bulkResponse.getItems()[3].isFailed());
        assertEquals(DocWriteRequest.OpType.INDEX, bulkResponse.getItems()[3].getOpType());
        //the missing index gets automatically created (user has permissions for that), but indexing fails due to missing authorization
        assertEquals("missing", bulkResponse.getItems()[3].getFailure().getIndex());
        assertThat(bulkResponse.getItems()[3].getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[3].getFailure().getCause());
        assertThat(bulkResponse.getItems()[3].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
        assertFalse(bulkResponse.getItems()[4].isFailed());
        assertEquals(DocWriteRequest.OpType.DELETE, bulkResponse.getItems()[4].getOpType());
        assertEquals("test1", bulkResponse.getItems()[4].getIndex());
        assertTrue(bulkResponse.getItems()[5].isFailed());
        assertEquals(DocWriteRequest.OpType.DELETE, bulkResponse.getItems()[5].getOpType());
        assertEquals("index1", bulkResponse.getItems()[5].getFailure().getIndex());
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[5].getFailure().getCause());
        assertThat(bulkResponse.getItems()[5].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
        assertFalse(bulkResponse.getItems()[6].isFailed());
        assertEquals(DocWriteRequest.OpType.DELETE, bulkResponse.getItems()[6].getOpType());
        assertEquals("test4", bulkResponse.getItems()[6].getIndex());
        assertTrue(bulkResponse.getItems()[7].isFailed());
        assertEquals(DocWriteRequest.OpType.DELETE, bulkResponse.getItems()[7].getOpType());
        assertEquals("missing", bulkResponse.getItems()[7].getFailure().getIndex());
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[7].getFailure().getCause());
        assertThat(bulkResponse.getItems()[7].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
        assertFalse(bulkResponse.getItems()[8].isFailed());
        assertEquals(DocWriteRequest.OpType.INDEX, bulkResponse.getItems()[8].getOpType());
        assertEquals("test1", bulkResponse.getItems()[8].getIndex());
        assertFalse(bulkResponse.getItems()[9].isFailed());
        assertEquals(DocWriteRequest.OpType.UPDATE, bulkResponse.getItems()[9].getOpType());
        assertEquals("test1", bulkResponse.getItems()[9].getIndex());
        assertTrue(bulkResponse.getItems()[10].isFailed());
        assertEquals(DocWriteRequest.OpType.UPDATE, bulkResponse.getItems()[10].getOpType());
        assertEquals("index1", bulkResponse.getItems()[10].getFailure().getIndex());
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[10].getFailure().getCause());
        assertThat(bulkResponse.getItems()[10].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
        assertTrue(bulkResponse.getItems()[11].isFailed());
        assertEquals(DocWriteRequest.OpType.UPDATE, bulkResponse.getItems()[11].getOpType());
        assertEquals("test4", bulkResponse.getItems()[11].getIndex());
        assertThat(bulkResponse.getItems()[11].getFailure().getCause(), instanceOf(DocumentMissingException.class));
        assertTrue(bulkResponse.getItems()[12].isFailed());
        assertEquals(DocWriteRequest.OpType.UPDATE, bulkResponse.getItems()[12].getOpType());
        assertEquals("missing", bulkResponse.getItems()[12].getFailure().getIndex());
        assertThat(bulkResponse.getItems()[12].getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertAuthorizationException((ElasticsearchSecurityException) bulkResponse.getItems()[12].getFailure().getCause());
        assertThat(bulkResponse.getItems()[12].getFailure().getCause().getMessage(),
                containsString("[indices:data/write/bulk[s]] is unauthorized"));
    }
}
