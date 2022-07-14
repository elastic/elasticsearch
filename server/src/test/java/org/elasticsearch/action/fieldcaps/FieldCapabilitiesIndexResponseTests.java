/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponseTests.randomFieldCaps;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesIndexResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesIndexResponse> {
    @Override
    protected Writeable.Reader<FieldCapabilitiesIndexResponse> instanceReader() {
        return in -> new FieldCapabilitiesIndexResponse(in, new IndexFieldCapabilities.Deduplicator());
    }

    static FieldCapabilitiesIndexResponse randomIndexResponse() {
        return randomIndexResponse(randomAsciiLettersOfLength(10), randomBoolean());
    }

    static FieldCapabilitiesIndexResponse randomIndexResponse(String index, boolean canMatch) {
        List<IndexFieldCapabilities> fields = new ArrayList<>();
        if (canMatch) {
            String[] fieldNames = generateRandomStringArray(5, 10, false, true);
            assertNotNull(fieldNames);
            for (String fieldName : fieldNames) {
                fields.add(randomFieldCaps(fieldName));
            }
        }
        return new FieldCapabilitiesIndexResponse(index, fields, canMatch);
    }

    @Override
    protected FieldCapabilitiesIndexResponse createTestInstance() {
        return randomIndexResponse();
    }

    public void testDeserializeFromBase64() throws Exception {
        String base64 = "CWxvZ3MtMTAwMQMGcGVyaW9kBnBlcmlvZARsb25nAQABAQR1bml0BnNlY29uZApAdGltZXN0"
            + "YW1wCkB0aW1lc3RhbXAEZGF0ZQEBAAAHbWVzc2FnZQdtZXNzYWdlBHRleHQAAQAAAQAAAAAAAAAAAA==";
        StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(base64));
        FieldCapabilitiesIndexResponse resp = new FieldCapabilitiesIndexResponse(in, new IndexFieldCapabilities.Deduplicator());
        assertTrue(resp.canMatch());
        assertThat(resp.getIndexName(), equalTo("logs-1001"));
        assertThat(
            resp.getFields(),
            containsInAnyOrder(
                new IndexFieldCapabilities("@timestamp", "date", true, true, false, Collections.emptyMap()),
                new IndexFieldCapabilities("message", "text", false, true, false, Collections.emptyMap()),
                new IndexFieldCapabilities("period", "long", true, false, true, Collections.singletonMap("unit", "second"))
            )
        );
    }
}
