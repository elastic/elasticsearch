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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractAsyncBulkIndexByScrollActionScriptTestCase<
                Request extends AbstractBulkIndexByScrollRequest<Request>,
                Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkIndexByScrollActionTestCase<Request, Response> {
    protected IndexRequest applyScript(Consumer<Map<String, Object>> scriptBody) {
        IndexRequest index = new IndexRequest("index", "type", "1").source(singletonMap("foo", "bar"));
        Map<String, SearchHitField> fields = new HashMap<>();
        InternalSearchHit doc = new InternalSearchHit(0, "id", new Text("type"), fields);
        doc.shardTarget(new SearchShardTarget("nodeid", new Index("index", "uuid"), 1));
        ExecutableScript script = new SimpleExecutableScript(scriptBody);
        action().applyScript(index, doc, script, new HashMap<>());
        return index;
    }

    public void testScriptAddingJunkToCtxIsError() {
        try {
            applyScript((Map<String, Object> ctx) -> ctx.put("junk", "junk"));
            fail("Expected error");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid fields added to ctx [junk]"));
        }
    }

    public void testChangeSource() {
        IndexRequest index = applyScript((Map<String, Object> ctx) -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
            source.put("bar", "cat");
        });
        assertEquals("cat", index.sourceAsMap().get("bar"));
    }
}
