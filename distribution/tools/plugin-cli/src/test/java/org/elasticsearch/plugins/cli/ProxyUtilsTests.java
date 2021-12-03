/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.test.ESTestCase;

import java.net.Proxy.Type;
import java.util.stream.Stream;

import static org.elasticsearch.plugins.cli.ProxyMatcher.matchesProxy;
import static org.elasticsearch.plugins.cli.ProxyUtils.buildProxy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ProxyUtilsTests extends ESTestCase {
    /**
     * Check that building a proxy with just a hostname and port succeeds.
     */
    public void testBuildProxy_withHostPort() throws Exception {
        assertThat(buildProxy("host:1234"), matchesProxy(Type.HTTP, "host", 1234));
    }

    /**
     * Check that building a proxy with a null value succeeds, returning a pass-through (direct) proxy.
     */
    public void testBuildProxy_withNullValue() throws Exception {
        assertThat(buildProxy(null), is(nullValue()));
    }

    /**
     * Check that building a proxy with a missing host is rejected.
     */
    public void testBuildProxy_withMissingHost() {
        UserException e = expectThrows(UserException.class, () -> buildProxy(":1234"));
        assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
    }

    /**
     * Check that building a proxy with a missing or invalid port is rejected.
     */
    public void testBuildProxy_withInvalidPort() {
        Stream.of("host:", "host.domain:-1", "host.domain:$PORT", "host.domain:{{port}}", "host.domain").forEach(testCase -> {
            UserException e = expectThrows(UserException.class, () -> buildProxy(testCase));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
        });
    }
}
