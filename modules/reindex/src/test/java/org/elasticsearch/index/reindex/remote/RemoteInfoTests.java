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

package org.elasticsearch.index.reindex.remote;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;

public class RemoteInfoTests extends ESTestCase {
    public void testToString() {
        RemoteInfo info = new RemoteInfo("http", "testhost", 12344, new BytesArray("testquery"), null, null, emptyMap());
        assertEquals("host=testhost port=12344 query=testquery", info.toString());
        info = new RemoteInfo("http", "testhost", 12344, new BytesArray("testquery"), "testuser", null, emptyMap());
        assertEquals("host=testhost port=12344 query=testquery username=testuser", info.toString());
        info = new RemoteInfo("http", "testhost", 12344, new BytesArray("testquery"), "testuser", "testpass", emptyMap());
        assertEquals("host=testhost port=12344 query=testquery username=testuser password=<<>>", info.toString());
        info = new RemoteInfo("https", "testhost", 12344, new BytesArray("testquery"), "testuser", "testpass", emptyMap());
        assertEquals("scheme=https host=testhost port=12344 query=testquery username=testuser password=<<>>", info.toString());
    }
}
