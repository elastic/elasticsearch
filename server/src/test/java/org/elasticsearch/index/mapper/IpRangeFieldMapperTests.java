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
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class IpRangeFieldMapperTests extends ESSingleNodeTestCase {

    private IndexService indexService;
    private DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testStoreCidr() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", "ip_range")
            .field("store", true);
        mapping = mapping.endObject().endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping.string()));
        assertEquals(mapping.string(), mapper.mappingSource().toString());
        final Map<String, String> cases = new HashMap<>();
        cases.put("192.168.0.0/15", "192.169.255.255");
        cases.put("192.168.0.0/16", "192.168.255.255");
        cases.put("192.168.0.0/17", "192.168.127.255");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            ParsedDocument doc =
                mapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", entry.getKey())
                        .endObject().bytes(),
                    XContentType.JSON
                ));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(3, fields.length);
            IndexableField dvField = fields[0];
            assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
            IndexableField pointField = fields[1];
            assertEquals(2, pointField.fieldType().pointDimensionCount());
            IndexableField storedField = fields[2];
            assertTrue(storedField.fieldType().stored());
            String strVal =
                InetAddresses.toAddrString(InetAddresses.forString("192.168.0.0")) + " : " +
                    InetAddresses.toAddrString(InetAddresses.forString(entry.getValue()));
            assertThat(storedField.stringValue(), containsString(strVal));
        }
    }
}
