/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthorizationExceptionDefaultUsers;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionDefaultUsers;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class WriteActionsTests extends SecurityIntegTestCase {

    @Override
    protected String configRoles() {
        return Strings.format("""
            %s:
              cluster: [ ALL ]
              indices:
                - names: 'missing'
                  privileges: [ 'indices:admin/create', 'indices:admin/auto_create', 'indices:admin/delete' ]
                - names: ['/index.*/']
                  privileges: [ manage ]
                - names: ['/test.*/']
                  privileges: [ manage, write ]
                - names: '/test.*/'
                  privileges: [ read ]
            """, SecuritySettingsSource.TEST_ROLE);
    }

    public void testIndex() {
        createIndex("test1", "index1");
        client().prepareIndex("test1").setId("id").setSource("field", "value").get();

        assertThrowsAuthorizationExceptionDefaultUsers(
            client().prepareIndex("index1").setId("id").setSource("field", "value")::get,
            BulkAction.NAME + "[s]"
        );

        client().prepareIndex("test4").setId("id").setSource("field", "value").get();
        // the missing index gets automatically created (user has permissions for that), but indexing fails due to missing authorization
        assertThrowsAuthorizationExceptionDefaultUsers(
            client().prepareIndex("missing").setId("id").setSource("field", "value")::get,
            BulkAction.NAME + "[s]"
        );
        ensureGreen();
    }

    public void testDelete() {
        createIndex("test1", "index1");
        client().prepareIndex("test1").setId("id").setSource("field", "value").get();
        assertEquals(RestStatus.OK, client().prepareDelete("test1", "id").get().status());

        assertThrowsAuthorizationExceptionDefaultUsers(client().prepareDelete("index1", "id")::get, BulkAction.NAME + "[s]");

        expectThrows(IndexNotFoundException.class, () -> client().prepareDelete("test4", "id").get());
        ensureGreen();
    }

    public void testUpdate() {
        createIndex("test1", "index1");
        client().prepareIndex("test1").setId("id").setSource("field", "value").get();
        assertEquals(
            RestStatus.OK,
            client().prepareUpdate("test1", "id").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2").get().status()
        );

        assertThrowsAuthorizationExceptionDefaultUsers(
            client().prepareUpdate("index1", "id").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2")::get,
            UpdateAction.NAME
        );

        expectThrows(
            DocumentMissingException.class,
            () -> client().prepareUpdate("test4", "id").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2").get()
        );

        assertThrowsAuthorizationExceptionDefaultUsers(
            client().prepareUpdate("missing", "id").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2")::get,
            UpdateAction.NAME
        );
        ensureGreen();
    }

    public void testBulk() {
        createIndex("test1", "test2", "test3", "index1");
        BulkResponse bulkResponse = client().prepareBulk()
            .add(new IndexRequest("test1").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new IndexRequest("index1").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new IndexRequest("test4").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new IndexRequest("missing").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new DeleteRequest("test1", "id"))
            .add(new DeleteRequest("index1", "id"))
            .add(new DeleteRequest("test4", "id"))
            .add(new DeleteRequest("missing", "id"))
            .add(new IndexRequest("test1").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new UpdateRequest("test1", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new UpdateRequest("index1", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new UpdateRequest("test4", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .add(new UpdateRequest("missing", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"))
            .get();
        assertTrue(bulkResponse.hasFailures());
        assertThat(bulkResponse.getItems().length, equalTo(13));
        assertThat(bulkResponse.getItems()[0].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[0].getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertThat(bulkResponse.getItems()[0].getIndex(), equalTo("test1"));
        assertThat(bulkResponse.getItems()[1].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[1].getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertThat(bulkResponse.getItems()[1].getFailure().getIndex(), equalTo("index1"));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[1].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[1].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        assertThat(bulkResponse.getItems()[2].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertThat(bulkResponse.getItems()[2].getResponse().getIndex(), equalTo("test4"));
        assertThat(bulkResponse.getItems()[3].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[3].getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        // the missing index gets automatically created (user has permissions for that), but indexing fails due to missing authorization
        assertThat(bulkResponse.getItems()[3].getFailure().getIndex(), equalTo("missing"));
        assertThat(bulkResponse.getItems()[3].getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[3].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[3].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        assertThat(bulkResponse.getItems()[4].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[4].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[4].getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertThat(bulkResponse.getItems()[4].getIndex(), equalTo("test1"));
        assertThat(bulkResponse.getItems()[5].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[5].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[5].getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertThat(bulkResponse.getItems()[5].getFailure().getIndex(), equalTo("index1"));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[5].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[5].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        assertThat(bulkResponse.getItems()[6].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[6].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[6].getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertThat(bulkResponse.getItems()[6].getIndex(), equalTo("test4"));
        assertThat(bulkResponse.getItems()[7].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[7].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[7].getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertThat(bulkResponse.getItems()[7].getFailure().getIndex(), equalTo("missing"));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[7].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[7].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        assertThat(bulkResponse.getItems()[8].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[8].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[8].getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertThat(bulkResponse.getItems()[8].getIndex(), equalTo("test1"));
        assertThat(bulkResponse.getItems()[9].getFailure(), nullValue());
        assertThat(bulkResponse.getItems()[9].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[9].getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(bulkResponse.getItems()[9].getIndex(), equalTo("test1"));
        assertThat(bulkResponse.getItems()[10].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[10].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[10].getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(bulkResponse.getItems()[10].getFailure().getIndex(), equalTo("index1"));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[10].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[10].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        assertThat(bulkResponse.getItems()[11].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[11].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[11].getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(bulkResponse.getItems()[11].getIndex(), equalTo("test4"));
        assertThat(bulkResponse.getItems()[11].getFailure().getCause(), instanceOf(DocumentMissingException.class));
        assertThat(bulkResponse.getItems()[12].getFailure(), notNullValue());
        assertThat(bulkResponse.getItems()[12].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[12].getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(bulkResponse.getItems()[12].getFailure().getIndex(), equalTo("missing"));
        assertThat(bulkResponse.getItems()[12].getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertAuthorizationExceptionDefaultUsers(bulkResponse.getItems()[12].getFailure().getCause(), BulkAction.NAME + "[s]");
        assertThat(
            bulkResponse.getItems()[12].getFailure().getCause().getMessage(),
            containsString("[indices:data/write/bulk[s]] is unauthorized")
        );
        ensureGreen();
    }
}
