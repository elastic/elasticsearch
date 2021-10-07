/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli.action;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.net.Proxy.Type;
import java.util.stream.Stream;

import static org.elasticsearch.plugins.cli.action.ProxyMatcher.matchesProxy;
import static org.elasticsearch.plugins.cli.action.ProxyUtils.buildProxy;
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
     * Check that building a proxy with a hostname with domain and a port succeeds.
     */
    public void testBuildProxy_withHostDomainPort() throws Exception {
        assertThat(buildProxy("host.localhost:1234"), matchesProxy(Type.HTTP, "host.localhost", 1234));
    }

    /**
     * Check that building a proxy with a null value succeeds, returning a pass-through (direct) proxy.
     */
    public void testBuildProxy_withNullValue() throws Exception {
        assertThat(buildProxy(null), is(nullValue()));
    }

    /**
     * Check that building a proxy with an invalid host is rejected.
     */
    public void testBuildProxy_withInvalidHost() {
        Stream.of("blah_blah:1234", "-host.domain:1234", "host.-domain:1234", "tést:1234", ":1234").forEach(testCase -> {
            UserException e = expectThrows(UserException.class, () -> buildProxy(testCase));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
        });
    }

    /**
     * Check that building a proxy with an invalid port is rejected.
     */
    public void testBuildProxy_withInvalidPort() {
        Stream.of("host.domain:-1", "host.domain:$PORT", "host.domain:{{port}}", "host.domain").forEach(testCase -> {
            UserException e = expectThrows(UserException.class, () -> buildProxy(testCase));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
        });
    }
}
