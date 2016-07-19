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

package org.elasticsearch.action;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class DocWriteResponseTests extends ESTestCase {
    public void testGetLocation() {
        DocWriteResponse response = new DocWriteResponse(new ShardId("index", "uuid", 0), "type", "id", 0) {
            // DocWriteResponse is abstract so we have to sneak a subclass in here to test it.
        };
        assertEquals("/index/type/id", response.getLocation(null));
        assertEquals("/index/type/id?routing=test_routing", response.getLocation("test_routing"));
    }
}
