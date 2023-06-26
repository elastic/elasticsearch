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
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class IpRangeFieldMapperTests extends RangeFieldMapperTests {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "ip_range");
    }

    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return in.startObject("field").field("gt", "::ffff:c0a8:107").field("lt", "2001:db8::").endObject();
    }

    @Override
    protected String storedValue() {
        return InetAddresses.toAddrString(InetAddresses.forString("192.168.1.7"))
            + " : "
            + InetAddresses.toAddrString(InetAddresses.forString("2001:db8:0:0:0:0:0:0"));
    }

    @Override
    protected boolean supportsCoerce() {
        return false;
    }

    @Override
    protected Object rangeValue() {
        return "192.168.1.7";
    }

    @Override
    protected boolean supportsDecimalCoerce() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    public void testStoreCidr() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "ip_range").field("store", true)));

        final Map<String, String> cases = new HashMap<>();
        cases.put("192.168.0.0/15", "192.169.255.255");
        cases.put("192.168.0.0/16", "192.168.255.255");
        cases.put("192.168.0.0/17", "192.168.127.255");
        for (final Map.Entry<String, String> entry : cases.entrySet()) {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", entry.getKey())));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(3, fields.size());
            IndexableField dvField = fields.get(0);
            assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
            IndexableField pointField = fields.get(1);
            assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
            IndexableField storedField = fields.get(2);
            assertTrue(storedField.fieldType().stored());
            String strVal = InetAddresses.toAddrString(InetAddresses.forString("192.168.0.0"))
                + " : "
                + InetAddresses.toAddrString(InetAddresses.forString(entry.getValue()));
            assertThat(storedField.stringValue(), containsString(strVal));
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
