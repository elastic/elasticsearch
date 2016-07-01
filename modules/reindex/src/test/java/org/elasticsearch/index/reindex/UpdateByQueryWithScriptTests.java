/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.script.ScriptService;

import java.util.Date;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class UpdateByQueryWithScriptTests
        extends AbstractAsyncBulkIndexByScrollActionScriptTestCase<UpdateByQueryRequest, BulkIndexByScrollResponse> {

    public void testModifyingCtxNotAllowed() {
        /*
         * Its important that none of these actually match any of the fields.
         * They don't now, but make sure they still don't match if you add any
         * more. The point of have many is that they should all present the same
         * error message to the user, not some ClassCastException.
         */
        Object[] options = new Object[] {"cat", new Object(), 123, new Date(), Math.PI};
        for (String ctxVar: new String[] {"_index", "_type", "_id", "_version", "_parent", "_routing", "_timestamp", "_ttl"}) {
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
    protected AbstractAsyncBulkIndexByScrollAction<UpdateByQueryRequest> action(ScriptService scriptService, UpdateByQueryRequest request) {
        return new TransportUpdateByQueryAction.AsyncIndexBySearchAction(task, logger, null, threadPool, request, listener(),
                scriptService, null);
    }
}
