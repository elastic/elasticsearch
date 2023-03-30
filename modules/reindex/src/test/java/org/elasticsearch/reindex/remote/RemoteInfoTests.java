/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex.remote;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;

public class RemoteInfoTests extends ESTestCase {
    private RemoteInfo newRemoteInfo(String scheme, String prefixPath, String username, String password) {
        return new RemoteInfo(
            scheme,
            "testhost",
            12344,
            prefixPath,
            new BytesArray("{ \"foo\" : \"bar\" }"),
            username,
            password == null ? null : new SecureString(password.toCharArray()),
            emptyMap(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
    }

    public void testToString() {
        assertEquals("host=testhost port=12344 query={ \"foo\" : \"bar\" }", newRemoteInfo("http", null, null, null).toString());
        assertEquals(
            "host=testhost port=12344 query={ \"foo\" : \"bar\" } username=testuser",
            newRemoteInfo("http", null, "testuser", null).toString()
        );
        assertEquals(
            "host=testhost port=12344 query={ \"foo\" : \"bar\" } username=testuser password=<<>>",
            newRemoteInfo("http", null, "testuser", "testpass").toString()
        );
        assertEquals(
            "scheme=https host=testhost port=12344 query={ \"foo\" : \"bar\" } username=testuser password=<<>>",
            newRemoteInfo("https", null, "testuser", "testpass").toString()
        );
        assertEquals(
            "scheme=https host=testhost port=12344 pathPrefix=prxy query={ \"foo\" : \"bar\" } username=testuser password=<<>>",
            newRemoteInfo("https", "prxy", "testuser", "testpass").toString()
        );
    }
}
