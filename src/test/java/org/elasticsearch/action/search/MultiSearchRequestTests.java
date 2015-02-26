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

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class MultiSearchRequestTests extends ElasticsearchTestCase {

    @Test
    public void simpleAdd() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/search/simple-msearch1.json");
        MultiSearchRequest request = new MultiSearchRequest().add(data, 0, data.length, null, null, null);
        assertThat(request.requests().size(), equalTo(8));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(0).indicesOptions(), equalTo(IndicesOptions.fromOptions(true, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(0).types().length, equalTo(0));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(request.requests().get(1).indicesOptions(), equalTo(IndicesOptions.fromOptions(false, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test"));
        assertThat(request.requests().get(2).indicesOptions(), equalTo(IndicesOptions.fromOptions(false, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(3).indices()[0], equalTo("test"));
        assertThat(request.requests().get(3).indicesOptions(), equalTo(IndicesOptions.fromOptions(true, true, true, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(4).indices()[0], equalTo("test"));
        assertThat(request.requests().get(4).indicesOptions(), equalTo(IndicesOptions.fromOptions(true, false, false, true, IndicesOptions.strictExpandOpenAndForbidClosed())));
        assertThat(request.requests().get(5).indices(), nullValue());
        assertThat(request.requests().get(5).types().length, equalTo(0));
        assertThat(request.requests().get(6).indices(), nullValue());
        assertThat(request.requests().get(6).types().length, equalTo(0));
        assertThat(request.requests().get(6).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(7).indices(), nullValue());
        assertThat(request.requests().get(7).types().length, equalTo(0));
    }

    @Test
    public void simpleAdd2() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/search/simple-msearch2.json");
        MultiSearchRequest request = new MultiSearchRequest().add(data, 0, data.length, null, null, null);
        assertThat(request.requests().size(), equalTo(5));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(0).types().length, equalTo(0));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices(), nullValue());
        assertThat(request.requests().get(2).types().length, equalTo(0));
        assertThat(request.requests().get(3).indices(), nullValue());
        assertThat(request.requests().get(3).types().length, equalTo(0));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(4).indices(), nullValue());
        assertThat(request.requests().get(4).types().length, equalTo(0));
    }
    
    @Test
    public void simpleAdd3() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/search/simple-msearch3.json");
        MultiSearchRequest request = new MultiSearchRequest().add(data, 0, data.length, null, null, null);
        assertThat(request.requests().size(), equalTo(4));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).types()[0], equalTo("type2"));
        assertThat(request.requests().get(2).types()[1], equalTo("type1"));
        assertThat(request.requests().get(3).indices(), nullValue());
        assertThat(request.requests().get(3).types().length, equalTo(0));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
    }

    @Test
    public void simpleAdd4() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/search/simple-msearch4.json");
        MultiSearchRequest request = new MultiSearchRequest().add(data, 0, data.length, null, null, null);
        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).queryCache(), equalTo(true));
        assertThat(request.requests().get(0).preference(), nullValue());
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).types()[0], equalTo("type1"));
        assertThat(request.requests().get(1).queryCache(), nullValue());
        assertThat(request.requests().get(1).preference(), equalTo("_local"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).types()[0], equalTo("type2"));
        assertThat(request.requests().get(2).types()[1], equalTo("type1"));
        assertThat(request.requests().get(2).routing(), equalTo("123"));
    }
}
