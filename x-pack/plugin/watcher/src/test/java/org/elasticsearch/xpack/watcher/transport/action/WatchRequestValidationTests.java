/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WatchRequestValidationTests extends ESTestCase {

    public void testAcknowledgeWatchInvalidWatchId()  {
        ActionRequestValidationException e = new AckWatchRequest("id with whitespaces").validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testAcknowledgeWatchInvalidActionId() {
        ActionRequestValidationException e = new AckWatchRequest("_id", "action id with whitespaces").validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("action id [action id with whitespaces] contains whitespace"));
    }

    public void testAcknowledgeWatchNullActionArray() {
        // need this to prevent some compilation errors, i.e. in 1.8.0_91
        String [] nullArray = null;
        ActionRequestValidationException e = new AckWatchRequest("_id", nullArray).validate();
        assertThat(e, is(nullValue()));
    }

    public void testAcknowledgeWatchNullActionId() {
        ActionRequestValidationException e = new AckWatchRequest("_id", new String[] {null}).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("action id may not be null"));
    }

    public void testActivateWatchInvalidWatchId() {
        ActionRequestValidationException e = new ActivateWatchRequest("id with whitespaces", randomBoolean()).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testDeleteWatchInvalidWatchId() {
        ActionRequestValidationException e = new DeleteWatchRequest("id with whitespaces").validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testDeleteWatchNullId() {
        ActionRequestValidationException e = new DeleteWatchRequest().validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id is missing"));
    }

    public void testPutWatchInvalidWatchId() {
        ActionRequestValidationException e = new PutWatchRequest("id with whitespaces", BytesArray.EMPTY, XContentType.JSON).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testPutWatchNullId() {
        ActionRequestValidationException e = new PutWatchRequest(null, BytesArray.EMPTY, XContentType.JSON).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id is missing"));
    }

    public void testPutWatchSourceNull() {
        ActionRequestValidationException e = new PutWatchRequest("foo", (BytesReference) null, XContentType.JSON).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch source is missing"));
    }

    public void testPutWatchContentNull() {
        ActionRequestValidationException e = new PutWatchRequest("foo", BytesArray.EMPTY, null).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("request body is missing"));
    }

    public void testGetWatchInvalidWatchId() {
        ActionRequestValidationException e = new GetWatchRequest("id with whitespaces").validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testGetWatchNullId() {
        ActionRequestValidationException e = new GetWatchRequest((String) null).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id is missing"));
    }

    public void testExecuteWatchInvalidWatchId() {
        ActionRequestValidationException e = new ExecuteWatchRequest("id with whitespaces").validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("watch id contains whitespace"));
    }

    public void testExecuteWatchMissingWatchIdNoSource() {
        ActionRequestValidationException e = new ExecuteWatchRequest((String) null).validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(),
                hasItem("a watch execution request must either have a watch id or an inline watch source, but both are missing"));
    }

    public void testExecuteWatchInvalidActionId() {
        ExecuteWatchRequest request = new ExecuteWatchRequest("foo");
        request.setActionMode("foo bar baz", ActionExecutionMode.EXECUTE);
        ActionRequestValidationException e = request.validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("action id [foo bar baz] contains whitespace"));
    }

    public void testExecuteWatchWatchIdAndSource() {
        ExecuteWatchRequest request = new ExecuteWatchRequest("foo");
        request.setWatchSource(BytesArray.EMPTY, XContentType.JSON);
        ActionRequestValidationException e = request.validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(),
                hasItem("a watch execution request must either have a watch id or an inline watch source but not both"));
    }

    public void testExecuteWatchSourceAndRecordExecution() {
        ExecuteWatchRequest request = new ExecuteWatchRequest();
        request.setWatchSource(BytesArray.EMPTY, XContentType.JSON);
        request.setRecordExecution(true);
        ActionRequestValidationException e = request.validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("the execution of an inline watch cannot be recorded"));
    }

    public void testExecuteWatchNullActionMode() {
        ExecuteWatchRequest request = new ExecuteWatchRequest();
        request.setActionMode(null, ActionExecutionMode.EXECUTE);
        ActionRequestValidationException e = request.validate();
        assertThat(e, is(notNullValue()));
        assertThat(e.validationErrors(), hasItem("action id may not be null"));
    }
}
