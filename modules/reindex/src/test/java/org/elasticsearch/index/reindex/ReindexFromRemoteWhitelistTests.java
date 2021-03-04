/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.index.reindex.ReindexValidator.buildRemoteWhitelist;
import static org.elasticsearch.index.reindex.ReindexValidator.checkRemoteWhitelist;

/**
 * Tests the reindex-from-remote whitelist of remotes.
 */
public class ReindexFromRemoteWhitelistTests extends ESTestCase {

    private final BytesReference query = new BytesArray("{ \"foo\" : \"bar\" }");

    public void testLocalRequestWithoutWhitelist() {
        checkRemoteWhitelist(buildRemoteWhitelist(emptyList()), null);
    }

    public void testLocalRequestWithWhitelist() {
        checkRemoteWhitelist(buildRemoteWhitelist(randomWhitelist()), null);
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String host, int port) {
        return new RemoteInfo(randomAlphaOfLength(5), host, port, null, query, null, null, emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
    }

    public void testWhitelistedRemote() {
        List<String> whitelist = randomWhitelist();
        String[] inList = whitelist.iterator().next().split(":");
        String host = inList[0];
        int port = Integer.valueOf(inList[1]);
        checkRemoteWhitelist(buildRemoteWhitelist(whitelist), newRemoteInfo(host, port));
    }

    public void testWhitelistedByPrefix() {
        checkRemoteWhitelist(buildRemoteWhitelist(singletonList("*.example.com:9200")),
                new RemoteInfo(randomAlphaOfLength(5), "es.example.com", 9200, null, query, null, null, emptyMap(),
                        RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT));
        checkRemoteWhitelist(buildRemoteWhitelist(singletonList("*.example.com:9200")),
                newRemoteInfo("6e134134a1.us-east-1.aws.example.com", 9200));
    }

    public void testWhitelistedBySuffix() {
        checkRemoteWhitelist(buildRemoteWhitelist(singletonList("es.example.com:*")), newRemoteInfo("es.example.com", 9200));
    }

    public void testWhitelistedByInfix() {
        checkRemoteWhitelist(buildRemoteWhitelist(singletonList("es*.example.com:9200")), newRemoteInfo("es1.example.com", 9200));
    }

    public void testLoopbackInWhitelistRemote() throws UnknownHostException {
        List<String> whitelist = randomWhitelist();
        whitelist.add("127.0.0.1:*");
        checkRemoteWhitelist(buildRemoteWhitelist(whitelist), newRemoteInfo("127.0.0.1", 9200));
    }

    public void testUnwhitelistedRemote() {
        int port = between(1, Integer.MAX_VALUE);
        List<String> whitelist = randomBoolean() ? randomWhitelist() : emptyList();
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> checkRemoteWhitelist(buildRemoteWhitelist(whitelist), newRemoteInfo("not in list", port)));
        assertEquals("[not in list:" + port + "] not whitelisted in reindex.remote.whitelist", e.getMessage());
    }

    public void testRejectMatchAll() {
        assertMatchesTooMuch(singletonList("*"));
        assertMatchesTooMuch(singletonList("**"));
        assertMatchesTooMuch(singletonList("***"));
        assertMatchesTooMuch(Arrays.asList("realstuff", "*"));
        assertMatchesTooMuch(Arrays.asList("*", "realstuff"));
        List<String> random = randomWhitelist();
        random.add("*");
        assertMatchesTooMuch(random);
    }

    public void testIPv6Address() {
        List<String> whitelist = randomWhitelist();
        whitelist.add("[::1]:*");
        checkRemoteWhitelist(buildRemoteWhitelist(whitelist), newRemoteInfo("[::1]", 9200));
    }

    private void assertMatchesTooMuch(List<String> whitelist) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> buildRemoteWhitelist(whitelist));
        assertEquals("Refusing to start because whitelist " + whitelist + " accepts all addresses. "
                + "This would allow users to reindex-from-remote any URL they like effectively having Elasticsearch make HTTP GETs "
                + "for them.", e.getMessage());
    }

    private List<String> randomWhitelist() {
        int size = between(1, 100);
        List<String> whitelist = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            whitelist.add(randomAlphaOfLength(5) + ':' + between(1, Integer.MAX_VALUE));
        }
        return whitelist;
    }
}
