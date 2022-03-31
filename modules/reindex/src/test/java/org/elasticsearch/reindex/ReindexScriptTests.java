/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.ScriptService;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests index-by-search with a script modifying the documents.
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
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Can't reindex without a destination index!"));
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
        Object junkVersion = randomFrom(new Object[] { "junk", Math.PI });
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("_version", junkVersion));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_version may only be set to an int or a long but was ["));
            assertThat(e.getMessage(), containsString(junkVersion.toString()));
        }
    }

    public void testSetRouting() throws Exception {
        String routing = randomRealisticUnicodeOfLengthBetween(5, 20);
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_routing", routing));
        assertEquals(routing, index.routing());
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest();
    }

    @Override
    protected Reindexer.AsyncIndexBySearchAction action(ScriptService scriptService, ReindexRequest request) {
        ReindexSslConfig sslConfig = Mockito.mock(ReindexSslConfig.class);
        return new Reindexer.AsyncIndexBySearchAction(task, logger, null, null, threadPool, scriptService, sslConfig, request, listener());
    }
}
