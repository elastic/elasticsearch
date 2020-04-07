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

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchSourcePhaseTests extends ESTestCase {

    public void testFetchSource() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecute(source, true, null, null);
        assertEquals(Collections.singletonMap("field","value"), hitContext.hit().getSourceAsMap());
    }

    public void testBasicFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field1", "value")
            .field("field2", "value2")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecute(source, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "field1", null);
        assertEquals(Collections.singletonMap("field1","value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "hello", null);
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "*", "field2");
        assertEquals(Collections.singletonMap("field1","value"), hitContext.hit().getSourceAsMap());
    }

    public void testMultipleFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .field("field2", "value2")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecuteMultiple(source, true, new String[]{"*.notexisting", "field"}, null);
        assertEquals(Collections.singletonMap("field","value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, new String[]{"field.notexisting.*", "field"}, null);
        assertEquals(Collections.singletonMap("field","value"), hitContext.hit().getSourceAsMap());
    }

    public void testNestedSource() throws IOException {
        Map<String, Object> expectedNested = Collections.singletonMap("nested2", Collections.singletonMap("field", "value0"));
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .field("field2", "value2")
            .field("nested1", expectedNested)
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecuteMultiple(source, true, null, null,
            new SearchHit.NestedIdentity("nested1", 0,null));
        assertEquals(expectedNested, hitContext.hit().getSourceAsMap());
        hitContext = hitExecuteMultiple(source, true, new String[]{"invalid"}, null,
            new SearchHit.NestedIdentity("nested1", 0,null));
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, null, null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null)));
        assertEquals(Collections.singletonMap("field", "value0"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, new String[]{"invalid"}, null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null)));
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());
    }

    public void testSourceDisabled() throws IOException {
        FetchSubPhase.HitContext hitContext = hitExecute(null, true, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(null, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> hitExecute(null, true, "field1", null));
        assertEquals("unable to fetch fields from _source field: _source is disabled in the mappings " +
                "for index [index]", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class,
                () -> hitExecuteMultiple(null, true, new String[]{"*"}, new String[]{"field2"}));
        assertEquals("unable to fetch fields from _source field: _source is disabled in the mappings " +
                "for index [index]", exception.getMessage());
    }

    public void testNestedSourceWithSourceDisabled() {
        FetchSubPhase.HitContext hitContext = hitExecute(null, true, null, null,
            new SearchHit.NestedIdentity("nested1", 0, null));
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> hitExecute(null, true, "field1", null, new SearchHit.NestedIdentity("nested1", 0, null)));
        assertEquals("unable to fetch fields from _source field: _source is disabled in the mappings " +
            "for index [index]", e.getMessage());
    }

    private FetchSubPhase.HitContext hitExecute(XContentBuilder source, boolean fetchSource, String include, String exclude) {
        return hitExecute(source, fetchSource, include, exclude, null);
    }


    private FetchSubPhase.HitContext hitExecute(XContentBuilder source, boolean fetchSource, String include, String exclude,
                                                    SearchHit.NestedIdentity nestedIdentity) {
        return hitExecuteMultiple(source, fetchSource,
            include == null ? Strings.EMPTY_ARRAY : new String[]{include},
            exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude}, nestedIdentity);
    }

    private FetchSubPhase.HitContext hitExecuteMultiple(XContentBuilder source, boolean fetchSource, String[] includes, String[] excludes) {
        return hitExecuteMultiple(source, fetchSource, includes, excludes, null);
    }

    private FetchSubPhase.HitContext hitExecuteMultiple(XContentBuilder source, boolean fetchSource, String[] includes, String[] excludes,
                                                            SearchHit.NestedIdentity nestedIdentity) {
        FetchSourceContext fetchSourceContext = new FetchSourceContext(fetchSource, includes, excludes);
        SearchContext searchContext = new FetchSourcePhaseTestSearchContext(fetchSourceContext,
                source == null ? null : BytesReference.bytes(source));
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        final SearchHit searchHit = new SearchHit(1, null, nestedIdentity, null, null);
        hitContext.reset(searchHit, null, 1, null);
        FetchSourcePhase phase = new FetchSourcePhase();
        phase.hitExecute(searchContext, hitContext);
        return hitContext;
    }

    private static class FetchSourcePhaseTestSearchContext extends TestSearchContext {
        final FetchSourceContext context;
        final BytesReference source;
        final IndexShard indexShard;

        FetchSourcePhaseTestSearchContext(FetchSourceContext context, BytesReference source) {
            super(null);
            this.context = context;
            this.source = source;
            this.indexShard = mock(IndexShard.class);
            when(indexShard.shardId()).thenReturn(new ShardId("index", "index", 1));
        }

        @Override
        public boolean sourceRequested() {
            return context != null && context.fetchSource();
        }

        @Override
        public FetchSourceContext fetchSourceContext() {
            return context;
        }

        @Override
        public SearchLookup lookup() {
            SearchLookup lookup = new SearchLookup(this.mapperService(), this::getForField);
            lookup.source().setSource(source);
            return lookup;
        }

        @Override
        public IndexShard indexShard() {
            return indexShard;
        }
    }
}
