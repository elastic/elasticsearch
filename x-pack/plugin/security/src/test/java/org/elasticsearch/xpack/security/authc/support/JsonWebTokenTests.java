/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebToken;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class JsonWebTokenTests extends ESTestCase {

    public void testUnsignedJwtEncoding() {
        Map<String, Object> header = new HashMap<>();
        Map<String, Object> payload = new HashMap<>();
        header.put("alg", "none");
        header.put("typ", "JWT");
        payload.put("sub", "ironman");
        payload.put("name", "Tony Stark");
        payload.put("iat", 1516239022L);
        final JsonWebToken jwt = new JsonWebToken(header, payload);
        // This is not an example of JWT validation. Order affects string representation
        // and string representation equality is no means to validate a JWT
        assertThat(jwt.encode(), equalTo("eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0" +
            ".eyJzdWIiOiJpcm9ubWFuIiwibmFtZSI6IlRvbnkgU3RhcmsiLCJpYXQiOjE1MTYyMzkwMjJ9."));
    }
}
