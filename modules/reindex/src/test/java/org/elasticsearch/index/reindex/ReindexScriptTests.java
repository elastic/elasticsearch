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

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.script.ScriptService;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests index-by-search with a script modifying the documents.
 */
public class ReindexScriptTests extends AbstractAsyncBulkByScrollActionScriptTestCase<ReindexRequest, BulkByScrollResponse> {

    public void testSetIndex() throws Exception {
        Object dest = randomFrom(new Object[] {234, 234L, "pancake"});
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

    public void testSetType() throws Exception {
        Object type = randomFrom(new Object[] {234, 234L, "pancake"});
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_type", type));
        assertEquals(type.toString(), index.type());
    }

    public void testSettingTypeToNullIsError() throws Exception {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("_type", null));
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("Can't reindex without a destination type!"));
        }
    }

    public void testSetId() throws Exception {
        Object id = randomFrom(new Object[] {null, 234, 234L, "pancake"});
        IndexRequest index = applyScript((Map<String, Object> ctx) -> ctx.put("_id", id));
        if (id == null) {
            assertNull(index.id());
        } else {
            assertEquals(id.toString(), index.id());
        }
    }

    public void testSetVersion() throws Exception {
        Number version = randomFrom(new Number[] {null, 234, 234L});
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
        return new Reindexer.AsyncIndexBySearchAction(task, logger, null, threadPool, scriptService, sslConfig, request, listener());
    }
}
