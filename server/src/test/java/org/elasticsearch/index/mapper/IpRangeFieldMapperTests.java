/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
