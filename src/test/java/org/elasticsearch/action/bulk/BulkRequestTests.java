/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.bulk;

import com.google.common.base.Charsets;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BulkRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleBulk1() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(Charsets.UTF_8), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
        assertThat(((IndexRequest) bulkRequest.requests().get(0)).source().toBytes(), equalTo(new BytesArray("{ \"field1\" : \"value1\" }").toBytes()));
        assertThat(bulkRequest.requests().get(1), instanceOf(DeleteRequest.class));
        assertThat(((IndexRequest) bulkRequest.requests().get(2)).source().toBytes(), equalTo(new BytesArray("{ \"field1\" : \"value3\" }").toBytes()));
    }

    @Test
    public void testSimpleBulk2() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk2.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(Charsets.UTF_8), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk3() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk3.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(Charsets.UTF_8), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk4() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk4.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(Charsets.UTF_8), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(4));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).id(), equalTo("1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).retryOnConflict(), equalTo(2));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).doc().source().toUtf8(), equalTo("{\"field\":\"value\"}"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).id(), equalTo("0"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).type(), equalTo("type1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).index(), equalTo("index1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).script(), equalTo("counter += param1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).scriptLang(), equalTo("js"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).scriptParams().size(), equalTo(1));
        assertThat(((Integer) ((UpdateRequest) bulkRequest.requests().get(1)).scriptParams().get("param1")), equalTo(1));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).upsertRequest().source().toUtf8(), equalTo("{\"counter\":1}"));
    }

}
