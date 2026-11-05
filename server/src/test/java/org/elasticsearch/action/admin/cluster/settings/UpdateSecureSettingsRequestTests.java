/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSecureSettingsRequestTests extends ESTestCase {

    public void testSerializationKeepsValuesAndRemovals() throws IOException {
        Map<String, SecureString> settings = new HashMap<>();
        settings.put("a.secret", new SecureString("value".toCharArray()));
        settings.put("b.removed", null);
        UpdateSecureSettingsRequest request = new UpdateSecureSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, settings);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                UpdateSecureSettingsRequest read = new UpdateSecureSettingsRequest(in);
                assertThat(read.settings().get("a.secret").toString(), equalTo("value"));
                assertTrue(read.settings().containsKey("b.removed"));
                assertThat(read.settings().get("b.removed"), nullValue());
            }
        }
    }

    public void testDescriptionsOmitValues() {
        UpdateSecureSettingsRequest request = new UpdateSecureSettingsRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            Map.of("a.secret", new SecureString("hunter2".toCharArray()))
        );
        assertThat(request.toString(), containsString("a.secret"));
        assertThat(request.toString(), not(containsString("hunter2")));
        assertThat(request.getDescription(), not(containsString("hunter2")));
    }

    public void testEmptyRequestIsInvalid() {
        assertThat(new UpdateSecureSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, Map.of()).validate(), notNullValue());
    }
}
