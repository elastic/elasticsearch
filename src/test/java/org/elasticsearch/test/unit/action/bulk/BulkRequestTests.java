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

package org.elasticsearch.test.unit.action.bulk;

import org.elasticsearch.action.bulk.BulkRequest;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BulkRequestTests {

    @Test
    public void testSimpleBulk1() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/test/unit/action/bulk/simple-bulk.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk2() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/test/unit/action/bulk/simple-bulk2.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk3() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/test/unit/action/bulk/simple-bulk3.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(), 0, bulkAction.length(), true, null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }
}
