/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.ScriptService;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests reindex with a script modifying the documents.
 */
public class ReindexScriptTests extends AbstractAsyncBulkByScrollActionScriptTestCase<ReindexRequest, BulkByScrollResponse> {

    public void testSetIndex() throws Exception {
        Object dest = randomFrom(new Object[] { 234, 234L, "pancake" });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_index", dest));
        assertEquals(dest.toString(), index.index());
    }

    public void testSettingIndexToNullIsError() throws Exception {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("_index", null));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_index cannot be null"));
        }
    }

    public void testSetId() throws Exception {
        Object id = randomFrom(new Object[] { null, 234, 234L, "pancake" });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_id", id));
        if (id == null) {
            assertNull(index.id());
        } else {
            assertEquals(id.toString(), index.id());
        }
    }

    public void testSetVersion() throws Exception {
        Number version = randomFrom(new Number[] { null, 234, 234L });
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_version", version));
        if (version == null) {
            assertEquals(Versions.MATCH_ANY, index.version());
        } else {
            assertEquals(version.longValue(), index.version());
        }
    }

    public void testSettingVersionToJunkIsAnError() throws Exception {
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> applyScript((Map<String, Object> ctx) -> ctx.put("_version", "junk"))
        );
        assertEquals(err.getMessage(), "_version [junk] is wrong type, expected assignable to [java.lang.Number], not [java.lang.String]");

        err = expectThrows(IllegalArgumentException.class, () -> applyScript((Map<String, Object> ctx) -> ctx.put("_version", Math.PI)));
        assertEquals(
            err.getMessage(),
            "_version may only be set to an int or a long but was [3.141592653589793] with type [java.lang.Double]"
        );
    }

    public void testSetRouting() throws Exception {
        String routing = randomRealisticUnicodeOfLengthBetween(5, 20);
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_routing", routing));
        assertEquals(routing, index.routing());
    }

    @Override
    protected ReindexRequest request() {
        ReindexRequest request = new ReindexRequest();
        request.getDestination().index("test");
        return request;
    }

    @Override
    protected Reindexer.AsyncIndexBySearchAction action(ScriptService scriptService, ReindexRequest request) {
        ReindexSslConfig sslConfig = Mockito.mock(ReindexSslConfig.class);
        return new Reindexer.AsyncIndexBySearchAction(
            task,
            logger,
            null,
            null,
            threadPool,
            scriptService,
            ClusterState.EMPTY_STATE,
            sslConfig,
            request,
            listener()
        );
    }
}
