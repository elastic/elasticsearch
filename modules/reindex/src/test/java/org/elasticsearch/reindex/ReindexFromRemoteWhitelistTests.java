/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.test.ESTestCase;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.reindex.ReindexValidator.checkRemoteWhitelist;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the reindex-from-remote whitelist of remotes.
 */
public class ReindexFromRemoteWhitelistTests extends ESTestCase {

    private final BytesReference query = new BytesArray("{ \"foo\" : \"bar\" }");

    public void testLocalRequestWithoutWhitelist() {
        checkRemoteWhitelist(ReindexValidator.buildRemoteWhitelist(Collections.emptyList(), List.of()), false, null);
    }

    public void testLocalRequestWithWhitelist() {
        checkRemoteWhitelist(ReindexValidator.buildRemoteWhitelist(randomWhitelist(), List.of()), false, null);
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String host, int port) {
        return new RemoteInfo(
            randomAlphaOfLength(5),
            host,
            port,
            null,
            query,
            null,
            null,
            emptyMap(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
    }

    public void testWhitelistedRemote() {
        List<String> whitelist = randomWhitelist();
        String[] inList = whitelist.iterator().next().split(":");
        String host = inList[0];
        int port = Integer.valueOf(inList[1]);
        checkRemoteWhitelist(ReindexValidator.buildRemoteWhitelist(whitelist, List.of()), false, newRemoteInfo(host, port));
    }

    public void testWhitelistedByPrefix() {
        checkRemoteWhitelist(
            ReindexValidator.buildRemoteWhitelist(Collections.singletonList("*.example.com:9200"), List.of()),
            false,
            new RemoteInfo(
                randomAlphaOfLength(5),
                "es.example.com",
                9200,
                null,
                query,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            )
        );
        checkRemoteWhitelist(
            ReindexValidator.buildRemoteWhitelist(Collections.singletonList("*.example.com:9200"), List.of()),
            false,
            newRemoteInfo("6e134134a1.us-east-1.aws.example.com", 9200)
        );
    }

    public void testWhitelistedBySuffix() {
        checkRemoteWhitelist(
            ReindexValidator.buildRemoteWhitelist(Collections.singletonList("es.example.com:*"), List.of()),
            false,
            newRemoteInfo("es.example.com", 9200)
        );
    }

    public void testWhitelistedByInfix() {
        checkRemoteWhitelist(
            ReindexValidator.buildRemoteWhitelist(Collections.singletonList("es*.example.com:9200"), List.of()),
            false,
            newRemoteInfo("es1.example.com", 9200)
        );
    }

    public void testLoopbackInWhitelistRemote() throws UnknownHostException {
        List<String> whitelist = randomWhitelist();
        whitelist.add("127.0.0.1:*");
        checkRemoteWhitelist(ReindexValidator.buildRemoteWhitelist(whitelist, List.of()), false, newRemoteInfo("127.0.0.1", 9200));
    }

    public void testUnwhitelistedRemote() {
        int port = between(1, Integer.MAX_VALUE);
        List<String> whitelist = randomBoolean() ? randomWhitelist() : emptyList();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> checkRemoteWhitelist(
                ReindexValidator.buildRemoteWhitelist(whitelist, List.of()),
                false,
                newRemoteInfo("not in list", port)
            )
        );
        assertThat(e.getMessage(), equalTo("[not in list:" + port + "] not whitelisted in reindex.remote.whitelist"));
    }

    public void testIPv6Address() {
        List<String> whitelist = randomWhitelist();
        whitelist.add("[::1]:*");
        checkRemoteWhitelist(ReindexValidator.buildRemoteWhitelist(whitelist, List.of()), false, newRemoteInfo("[::1]", 9200));
    }

    public void testRemoteUnaffectedByBlocklist() {
        checkRemoteWhitelist(
            ReindexValidator.buildRemoteWhitelist(List.of("*.example.com:9200"), List.of("*.qa.example.com:*")),
            true,
            newRemoteInfo("6e134134a1.us-east-1.aws.example.com", 9200)
        );
    }

    public void testRemoteAffectedByBlocklist() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> checkRemoteWhitelist(
                ReindexValidator.buildRemoteWhitelist(List.of("*.example.com:9200"), List.of("*.qa.example.com:*")),
                true,
                newRemoteInfo("6e134134a1.us-east-1.aws.qa.example.com", 9200)
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "[6e134134a1.us-east-1.aws.qa.example.com:9200] either not whitelisted in reindex.remote.whitelist or blocked in reindex.remote.blocklist"
            )
        );
    }

    public void testRemoteNeitherWhitelistedNorBlocklisted() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> checkRemoteWhitelist(
                ReindexValidator.buildRemoteWhitelist(List.of("*.example.com:9200"), List.of("*.qa.example.com:*")),
                true,
                newRemoteInfo("6e134134a1.us-east-1.aws.notexample.com", 9200)
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "[6e134134a1.us-east-1.aws.notexample.com:9200] either not whitelisted in reindex.remote.whitelist or blocked in reindex.remote.blocklist"
            )
        );
    }

    public void testRemoteBlocklistedButNotWhitelisted() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> checkRemoteWhitelist(
                ReindexValidator.buildRemoteWhitelist(List.of("*.example.com:9200"), List.of("*.notexample.com:*")),
                true,
                newRemoteInfo("6e134134a1.us-east-1.aws.notexample.com", 9200)
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "[6e134134a1.us-east-1.aws.notexample.com:9200] either not whitelisted in reindex.remote.whitelist or blocked in reindex.remote.blocklist"
            )
        );
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
