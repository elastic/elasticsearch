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
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchSourceSubPhaseTests extends ESTestCase {

    static class FetchSourceSubPhaseTestSearchContext extends TestSearchContext {

        FetchSourceContext context;
        BytesReference source;

        FetchSourceSubPhaseTestSearchContext(FetchSourceContext context, BytesReference source) {
            super(null);
            this.context = context;
            this.source = source;
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
            SearchLookup lookup = super.lookup();
            lookup.source().setSource(source);
            return lookup;
        }
    }

    public void test() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecute(source, true, null, null);
        Map<String, Object> expected = new HashMap<>();
        expected.put("field", "value");
        assertThat(hitContext.hit().sourceAsMap(), equalTo(expected));
    }

    public void testBasicFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field1", "value")
            .field("field2", "value2")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecute(source, false, null, null);
        assertThat(hitContext.hit().sourceAsMap(), nullValue());

        hitContext = hitExecute(source, true, "field1", null);
        Map<String, Object> expected = new HashMap<>();
        expected.put("field1", "value");
        assertThat(hitContext.hit().sourceAsMap(), equalTo(expected));

        hitContext = hitExecute(source, true, "hello", null);
        assertThat(hitContext.hit().sourceAsMap(), equalTo(Collections.emptyMap()));

        hitContext = hitExecute(source, true, "*", "field2");
        expected = new HashMap<>();
        expected.put("field1", "value");
        assertThat(hitContext.hit().sourceAsMap(), equalTo(expected));
    }

    public void testMultipleFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .field("field2", "value2")
            .endObject();
        FetchSubPhase.HitContext hitContext = hitExecuteMultiple(source, true, new String[]{"*.notexisting", "field"}, null);
        Map<String, Object> expected = new HashMap<>();
        expected.put("field", "value");
        assertThat(hitContext.hit().sourceAsMap(), equalTo(expected));

        hitContext = hitExecuteMultiple(source, true, new String[]{"field.notexisting.*", "field"}, null);
        expected = new HashMap<>();
        expected.put("field", "value");
        assertThat(hitContext.hit().sourceAsMap(), equalTo(expected));
    }

    public void testSourceDisabled() throws IOException {
        FetchSubPhase.HitContext hitContext = hitExecute(null, true, null, null);
        assertThat(hitContext.hit().sourceAsMap(), nullValue());

        hitContext = hitExecute(null, false, null, null);
        assertThat(hitContext.hit().sourceAsMap(), nullValue());

        hitContext = hitExecute(null, true, "field1", null);
        assertThat(hitContext.hit().sourceAsMap(), nullValue());

        hitContext = hitExecuteMultiple(null, true, new String[]{"*"}, new String[]{"field2"});
        assertThat(hitContext.hit().sourceAsMap(), nullValue());
    }

    private FetchSubPhase.HitContext hitExecute(XContentBuilder source, boolean fetchSource, String include, String exclude) {
        return hitExecuteMultiple(source, fetchSource,
            include == null ? Strings.EMPTY_ARRAY : new String[]{include},
            exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude});
    }

    private FetchSubPhase.HitContext hitExecuteMultiple(XContentBuilder source, boolean fetchSource, String[] includes, String[] excludes) {
        FetchSourceContext fetchSourceContext = new FetchSourceContext(fetchSource, includes, excludes);
        SearchContext searchContext = new FetchSourceSubPhaseTestSearchContext(fetchSourceContext, source == null ? null : source.bytes());
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        hitContext.reset(new InternalSearchHit(1, null, null, null), null, 1, null);
        FetchSourceSubPhase phase = new FetchSourceSubPhase();
        phase.hitExecute(searchContext, hitContext);
        return hitContext;
    }
}
