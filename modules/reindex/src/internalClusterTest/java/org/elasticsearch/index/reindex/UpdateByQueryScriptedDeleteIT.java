/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexTestCase;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.test.ESTestCase.safeAwait;

/** Verifies that a scripted delete cannot remove a document changed after the search snapshot. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class UpdateByQueryScriptedDeleteIT extends ReindexTestCase {
    private static final String DELETE_SCRIPT = "delete_after_concurrent_update";
    private static CountDownLatch scriptStarted;
    private static CountDownLatch allowDelete;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DeleteScriptPlugin.class);
    }

    public void testConcurrentUpdateSurvivesScriptedDelete() throws Exception {
        indexRandom(true, prepareIndex("test").setId("1").setSource("value", "original"));
        scriptStarted = new CountDownLatch(1);
        allowDelete = new CountDownLatch(1);
        var future = updateByQuery().source("test")
            .script(new Script(ScriptType.INLINE, MockScriptPlugin.NAME, DELETE_SCRIPT, Map.of()))
            .execute();
        try {
            safeAwait(scriptStarted);
            // The search has read the old revision, but its scripted delete has not been sent yet.
            prepareIndex("test").setId("1").setSource("value", "newer").get();
        } finally {
            allowDelete.countDown();
        }

        var response = future.get(30, TimeUnit.SECONDS);
        assertEquals(0L, response.getDeleted());
        assertEquals(1L, response.getVersionConflicts());
        assertEquals(1, response.getBulkFailures().size());
        assertEquals(CONFLICT, response.getBulkFailures().get(0).getStatus());
        var document = client().prepareGet("test", "1").get();
        assertTrue(document.isExists());
        assertEquals("newer", document.getSourceAsMap().get("value"));
    }

    /** Pauses after the search snapshot so the concurrent update has a deterministic position. */
    public static class DeleteScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(DELETE_SCRIPT, vars -> {
                scriptStarted.countDown();
                safeAwait(allowDelete);
                @SuppressWarnings("unchecked")
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                ctx.put("op", "delete");
                return null;
            });
        }
    }
}
