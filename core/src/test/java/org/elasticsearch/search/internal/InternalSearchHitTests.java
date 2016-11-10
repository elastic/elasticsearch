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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalSearchHitTests extends ESTestCase {

    public void testSerializeShardTarget() throws Exception {
        SearchShardTarget target = new SearchShardTarget("_node_id", new Index("_index", "_na_"), 0);

        Map<String, InternalSearchHits> innerHits = new HashMap<>();
        InternalSearchHit innerHit1 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit1.shardTarget(target);
        InternalSearchHit innerInnerHit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerInnerHit2.shardTarget(target);
        innerHits.put("1", new InternalSearchHits(new InternalSearchHit[]{innerInnerHit2}, 1, 1f));
        innerHit1.setInnerHits(innerHits);
        InternalSearchHit innerHit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit2.shardTarget(target);
        InternalSearchHit innerHit3 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHit3.shardTarget(target);

        innerHits = new HashMap<>();
        InternalSearchHit hit1 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        innerHits.put("1", new InternalSearchHits(new InternalSearchHit[]{innerHit1, innerHit2}, 1, 1f));
        innerHits.put("2", new InternalSearchHits(new InternalSearchHit[]{innerHit3}, 1, 1f));
        hit1.shardTarget(target);
        hit1.setInnerHits(innerHits);

        InternalSearchHit hit2 = new InternalSearchHit(0, "_id", new Text("_type"), null);
        hit2.shardTarget(target);

        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[]{hit1, hit2}, 2, 1f);

        BytesStreamOutput output = new BytesStreamOutput();
        hits.writeTo(output);
        InputStream input = output.bytes().streamInput();
        InternalSearchHits results = InternalSearchHits.readSearchHits(new InputStreamStreamInput(input));
        assertThat(results.getAt(0).shard(), equalTo(target));
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getInnerHits().get("1").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(1).shard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("2").getAt(0).shard(), notNullValue());
        assertThat(results.getAt(1).shard(), equalTo(target));
    }

    public void testNullSource() throws Exception {
        InternalSearchHit searchHit = new InternalSearchHit(0, "_id", new Text("_type"), null);

        assertThat(searchHit.source(), nullValue());
        assertThat(searchHit.sourceRef(), nullValue());
        assertThat(searchHit.sourceAsMap(), nullValue());
        assertThat(searchHit.sourceAsString(), nullValue());
        assertThat(searchHit.getSource(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
    }

    public void testHasSource() {
        InternalSearchHit searchHit = new InternalSearchHit(randomInt());
        assertFalse(searchHit.hasSource());
        searchHit.sourceRef(new BytesArray("{}"));
        assertTrue(searchHit.hasSource());
    }
}
