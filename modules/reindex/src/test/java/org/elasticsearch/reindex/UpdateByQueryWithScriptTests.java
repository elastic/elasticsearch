/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class UpdateByQueryWithScriptTests extends AbstractAsyncBulkByScrollActionScriptTestCase<
    UpdateByQueryRequest,
    BulkByScrollResponse> {

    public void testModifyingCtxNotAllowed() {
        /*
         * Its important that none of these actually match any of the fields.
         * They don't now, but make sure they still don't match if you add any
         * more. The point of have many is that they should all present the same
         * error message to the user, not some ClassCastException.
         */
        Object[] options = new Object[] { "cat", new Object(), 123, new Date(), Math.PI };
        for (String ctxVar : new String[] { "_index", "_id", "_version", "_routing" }) {
            try {
                applyScript((Map<String, Object> ctx) -> ctx.put(ctxVar, randomFrom(options)));
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Modifying [" + ctxVar + "] not allowed"));
            }
        }
    }

    @Override
    protected UpdateByQueryRequest request() {
        return new UpdateByQueryRequest();
    }

    @Override
    protected TransportUpdateByQueryAction.AsyncIndexBySearchAction action(ScriptService scriptService, UpdateByQueryRequest request) {
        TransportService transportService = mock(TransportService.class);
        TransportUpdateByQueryAction transportAction = new TransportUpdateByQueryAction(
            threadPool,
            new ActionFilters(Collections.emptySet()),
            null,
            transportService,
            scriptService,
            null
        );
        return new TransportUpdateByQueryAction.AsyncIndexBySearchAction(
            task,
            logger,
            null,
            threadPool,
            scriptService,
            request,
            ClusterState.EMPTY_STATE,
            listener()
        );
    }
}
