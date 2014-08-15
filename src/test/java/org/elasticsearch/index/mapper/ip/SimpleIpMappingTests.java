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

package org.elasticsearch.index.mapper.ip;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleIpMappingTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testSimpleMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("ip").field("type", "ip").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("ip", "127.0.0.1")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("ip").numericValue().longValue(), is(2130706433L));
        assertThat(doc.rootDoc().get("ip"), is(nullValue()));
    }

    @Test
    public void testThatValidIpCanBeConvertedToLong() throws Exception {
        assertThat(IpFieldMapper.ipToLong("127.0.0.1"), is(2130706433L));
    }

    @Test
    public void testThatInvalidIpThrowsException() throws Exception {
        try {
            IpFieldMapper.ipToLong("127.0.011.1111111");
            fail("Expected ip address parsing to fail but did not happen");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not a valid ip address"));
        }
    }

    @Test
    public void testThatIpv6AddressThrowsException() throws Exception {
        try {
            IpFieldMapper.ipToLong("2001:db8:0:8d3:0:8a2e:70:7344");
            fail("Expected ip address parsing to fail but did not happen");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not a valid ipv4 address"));
        }
    }

}
