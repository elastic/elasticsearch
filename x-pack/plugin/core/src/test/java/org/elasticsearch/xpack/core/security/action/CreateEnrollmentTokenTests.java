/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CreateEnrollmentTokenTests extends ESTestCase {

    public void testSerialization() throws Exception {
        CreateEnrollmentTokenResponse response = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                CreateEnrollmentTokenResponse serialized = new CreateEnrollmentTokenResponse(in);
                assertEquals(response.getEnrollmentToken(), serialized.getEnrollmentToken());
            }
        }
    }

    protected CreateEnrollmentTokenResponse createTestInstance() {
        final String jsonString = "{\"adr\":\"192.168.1.43:9201\",\"fgr\":\"" +
            "48:CC:6C:F8:76:43:3C:97:85:B6:24:45:5B:FF:BD:40:4B:D6:35:81:51:E7:A9:99:60:E4:0A:C8:8D:AE:5C:4D\",\"key\":\"" +
            "VuaCfGcBCdbkQm-e5aOx:ui2lp2axTNmsyakw9tvNnw\" }";

        final String token = Base64.getEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
        return new CreateEnrollmentTokenResponse(token);
    }
}
