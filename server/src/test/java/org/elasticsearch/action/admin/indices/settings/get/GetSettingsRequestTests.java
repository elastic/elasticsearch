/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class GetSettingsRequestTests extends ESTestCase {
    private static final GetSettingsRequest TEST_700_REQUEST = new GetSettingsRequest()
            .includeDefaults(true)
            .humanReadable(true)
            .indices("test_index")
            .names("test_setting_key");

    public void testSerdeRoundTrip() throws IOException {
        BytesStreamOutput bso = new BytesStreamOutput();
        TEST_700_REQUEST.writeTo(bso);

        byte[] responseBytes = BytesReference.toBytes(bso.bytes());
        StreamInput si = StreamInput.wrap(responseBytes);
        GetSettingsRequest deserialized = new GetSettingsRequest(si);
        assertEquals(TEST_700_REQUEST, deserialized);
    }
}
