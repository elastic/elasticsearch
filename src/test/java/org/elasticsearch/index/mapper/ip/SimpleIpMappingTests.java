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

package org.elasticsearch.index.mapper.ip;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Ignore;

/**
 *
 */
@Ignore("No tests?")
public class SimpleIpMappingTests extends ElasticsearchTestCase {

    // No Longer enabled...
//    @Test public void testAutoIpDetection() throws Exception {
//        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
//                .startObject("properties").endObject()
//                .endObject().endObject().string();
//
//        XContentDocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
//
//        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
//                .startObject()
//                .field("ip1", "127.0.0.1")
//                .field("ip2", "0.1")
//                .field("ip3", "127.0.0.1.2")
//                .endObject()
//                .copiedBytes());
//
//        assertThat(doc.doc().getFieldable("ip1"), notNullValue());
//        assertThat(doc.doc().get("ip1"), nullValue()); // its numeric
//        assertThat(doc.doc().get("ip2"), equalTo("0.1"));
//        assertThat(doc.doc().get("ip3"), equalTo("127.0.0.1.2"));
//    }
}
