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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Utilities for testing reindex and update-by-query cancelation. This whole class isn't thread safe. Luckily we run out tests in separate
 * jvms.
 */
public class CancelTestUtils {
    public static Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, StickyScriptPlugin.class);
    }

    private static final CyclicBarrier barrier = new CyclicBarrier(2);

    public static <Request extends AbstractBulkIndexByScrollRequest<Request>,
                    Response extends ActionResponse,
                    Builder extends AbstractBulkIndexByScrollRequestBuilder<Request, Response, Builder>>
            Response testCancel(ESIntegTestCase test, Builder request, String actionToCancel) throws Exception {

        test.indexRandom(true, client().prepareIndex("source", "test", "1").setSource("foo", "a"),
                client().prepareIndex("source", "test", "2").setSource("foo", "a"));

        request.source("source").script(new Script("sticky", ScriptType.INLINE, "native", emptyMap()));
        request.source().setSize(1);
        ListenableActionFuture<Response> response = request.execute();

        // Wait until the script is on the first document.
        barrier.await(30, TimeUnit.SECONDS);

        // Let just one document through.
        barrier.await(30, TimeUnit.SECONDS);

        // Wait until the script is on the second document.
        barrier.await(30, TimeUnit.SECONDS);

        // Cancel the request while the script is running. This will prevent the request from being sent at all.
        List<TaskInfo> cancelledTasks = client().admin().cluster().prepareCancelTasks().setActions(actionToCancel).get().getTasks();
        assertThat(cancelledTasks, hasSize(1));

        // Now let the next document through. It won't be sent because the request is cancelled but we need to unblock the script.
        barrier.await();

        // Now we can just wait on the request and make sure it was actually cancelled half way through.
        return response.get();
    }

    public static class StickyScriptPlugin extends Plugin {
        @Override
        public String name() {
            return "sticky-script";
        }

        @Override
        public String description() {
            return "installs a script that \"sticks\" when it runs for testing reindex";
        }

        public void onModule(ScriptModule module) {
            module.registerScript("sticky", StickyScriptFactory.class);
        }
    }

    public static class StickyScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            return new ExecutableScript() {
                private Map<String, Object> source;
                @Override
                @SuppressWarnings("unchecked") // Safe because _ctx always has this shape
                public void setNextVar(String name, Object value) {
                    if ("ctx".equals(name)) {
                        Map<String, Object> ctx = (Map<String, Object>) value;
                        source = (Map<String, Object>) ctx.get("_source");
                    } else {
                        throw new IllegalArgumentException("Unexpected var:  " + name);
                    }
                }

                @Override
                public Object run() {
                    try {
                        // Tell the test we've started a document.
                        barrier.await(30, TimeUnit.SECONDS);

                        // Wait for the test to tell us to proceed.
                        barrier.await(30, TimeUnit.SECONDS);

                        // Make some change to the source so that update-by-query tests can make sure only one document was changed.
                        source.put("giraffes", "giraffes");
                        return null;
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
