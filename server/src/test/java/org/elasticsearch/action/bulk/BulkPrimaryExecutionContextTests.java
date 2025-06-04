/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.TransportShardBulkActionTests.FakeDeleteResult;
import org.elasticsearch.action.bulk.TransportShardBulkActionTests.FakeIndexResult;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkPrimaryExecutionContextTests extends ESTestCase {

    public void testAbortedSkipped() {
        BulkShardRequest shardRequest = generateRandomRequest();

        ArrayList<DocWriteRequest<?>> nonAbortedRequests = new ArrayList<>();
        for (BulkItemRequest request : shardRequest.items()) {
            if (randomBoolean()) {
                request.abort("index", new ElasticsearchException("bla"));
            } else {
                nonAbortedRequests.add(request.request());
            }
        }

        ArrayList<DocWriteRequest<?>> visitedRequests = new ArrayList<>();
        for (BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(shardRequest, null); context
            .hasMoreOperationsToExecute();) {
            visitedRequests.add(context.getCurrent());
            context.setRequestToExecute(context.getCurrent());
            // using failures prevents caring about types
            context.markOperationAsExecuted(
                new Engine.IndexResult(new ElasticsearchException("bla"), 1, context.getRequestToExecute().id())
            );
            context.markAsCompleted(context.getExecutionResult());
        }

        assertThat(visitedRequests, equalTo(nonAbortedRequests));
    }

    private BulkShardRequest generateRandomRequest() {
        BulkItemRequest[] items = new BulkItemRequest[randomInt(20)];
        for (int i = 0; i < items.length; i++) {
            final DocWriteRequest<?> request = switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> new IndexRequest("index").id("id_" + i);
                case CREATE -> new IndexRequest("index").id("id_" + i).create(true);
                case UPDATE -> new UpdateRequest("index", "id_" + i);
                case DELETE -> new DeleteRequest("index", "id_" + i);
            };
            items[i] = new BulkItemRequest(i, request);
        }
        return new BulkShardRequest(new ShardId("index", "_na_", 0), randomFrom(WriteRequest.RefreshPolicy.values()), items);
    }

    public void testTranslogLocation() {

        BulkShardRequest shardRequest = generateRandomRequest();

        Translog.Location expectedLocation = null;
        final IndexShard primary = mock(IndexShard.class);
        when(primary.shardId()).thenReturn(shardRequest.shardId());

        long translogGen = 0;
        long translogOffset = 0;

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(shardRequest, primary);
        while (context.hasMoreOperationsToExecute()) {
            final Engine.Result result;
            final DocWriteRequest<?> current = context.getCurrent();
            final boolean failure = rarely();
            if (frequently()) {
                translogGen += randomIntBetween(1, 4);
                translogOffset = 0;
            } else {
                translogOffset += randomIntBetween(200, 400);
            }

            Translog.Location location = new Translog.Location(translogGen, translogOffset, randomInt(200));
            switch (current.opType()) {
                case INDEX, CREATE -> {
                    context.setRequestToExecute(current);
                    if (failure) {
                        result = new Engine.IndexResult(new ElasticsearchException("bla"), 1, current.id());
                    } else {
                        result = new FakeIndexResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, "id");
                    }
                }
                case UPDATE -> {
                    context.setRequestToExecute(new IndexRequest(current.index()).id(current.id()));
                    if (failure) {
                        result = new Engine.IndexResult(new ElasticsearchException("bla"), 1, 1, 1, current.id());
                    } else {
                        result = new FakeIndexResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, "id");
                    }
                }
                case DELETE -> {
                    context.setRequestToExecute(current);
                    if (failure) {
                        result = new Engine.DeleteResult(new ElasticsearchException("bla"), 1, 1, current.id());
                    } else {
                        result = new FakeDeleteResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, current.id());
                    }
                }
                default -> throw new AssertionError("unknown type:" + current.opType());
            }
            if (failure == false) {
                expectedLocation = location;
            }
            context.markOperationAsExecuted(result);
            context.markAsCompleted(context.getExecutionResult());
        }

        assertThat(context.getLocationToSync(), equalTo(expectedLocation));
    }
}
