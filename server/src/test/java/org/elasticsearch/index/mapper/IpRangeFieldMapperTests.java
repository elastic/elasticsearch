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
import org.elasticsearch.common.network.InetAddresses;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class IpRangeFieldMapperTests extends MapperServiceTestCase {

    public void testStoreCidr() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "ip_range").field("store", true)));

        final Map<String, String> cases = new HashMap<>();
        cases.put("192.168.0.0/15", "192.169.255.255");
        cases.put("192.168.0.0/16", "192.168.255.255");
        cases.put("192.168.0.0/17", "192.168.127.255");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            ParsedDocument doc =
                mapper.parse(source(b -> b.field("field", entry.getKey())));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(3, fields.length);
            IndexableField dvField = fields[0];
            assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
            IndexableField pointField = fields[1];
            assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
            IndexableField storedField = fields[2];
            assertTrue(storedField.fieldType().stored());
            String strVal =
                InetAddresses.toAddrString(InetAddresses.forString("192.168.0.0")) + " : " +
                    InetAddresses.toAddrString(InetAddresses.forString(entry.getValue()));
            assertThat(storedField.stringValue(), containsString(strVal));
        }
    }
}
