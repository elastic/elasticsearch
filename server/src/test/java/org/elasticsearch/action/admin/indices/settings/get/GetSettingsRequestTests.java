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

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;

public class GetSettingsRequestTests extends ESTestCase {
    private static final String TEST_622_REQUEST_BYTES = "ADwDAAEKdGVzdF9pbmRleA4BEHRlc3Rfc2V0dGluZ19rZXkB";
    private static final GetSettingsRequest TEST_622_REQUEST = new GetSettingsRequest()
        .indices("test_index")
        .names("test_setting_key")
        .humanReadable(true);
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
        GetSettingsRequest deserialized = new GetSettingsRequest();
        deserialized.readFrom(si);
        assertEquals(TEST_700_REQUEST, deserialized);
    }

    public void testSerializeBackwardsCompatibility() throws IOException {
        BytesStreamOutput bso = new BytesStreamOutput();
        bso.setVersion(Version.V_6_2_2);
        TEST_700_REQUEST.writeTo(bso);

        byte[] responseBytes = BytesReference.toBytes(bso.bytes());
        assertEquals(TEST_622_REQUEST_BYTES, Base64.getEncoder().encodeToString(responseBytes));
    }

    public void testDeserializeBackwardsCompatibility() throws IOException {
        StreamInput si = StreamInput.wrap(Base64.getDecoder().decode(TEST_622_REQUEST_BYTES));
        si.setVersion(Version.V_6_2_2);
        GetSettingsRequest deserialized = new GetSettingsRequest();
        deserialized.readFrom(si);
        assertEquals(TEST_622_REQUEST, deserialized);
    }
}
