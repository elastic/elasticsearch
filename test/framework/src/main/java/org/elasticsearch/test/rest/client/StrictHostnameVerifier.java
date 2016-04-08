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
package org.elasticsearch.test.rest.client;

import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.conn.util.InetAddressUtils;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.cert.X509Certificate;

/**
 * A custom {@link X509HostnameVerifier} implementation that wraps calls to the {@link org.apache.http.conn.ssl.StrictHostnameVerifier} and
 * properly handles IPv6 addresses that come from a URL in the form <code>http://[::1]:9200/</code> by removing the surrounding brackets.
 *
 * This is a variation of the fix for <a href="https://issues.apache.org/jira/browse/HTTPCLIENT-1698">HTTPCLIENT-1698</a>, which is not
 * released yet as of Apache HttpClient 4.5.1
 */
final class StrictHostnameVerifier implements X509HostnameVerifier {

    static final StrictHostnameVerifier INSTANCE = new StrictHostnameVerifier();

    // We need to wrap the default verifier for HttpClient since we use an older version and the following issue is not
    // fixed in a released version yet https://issues.apache.org/jira/browse/HTTPCLIENT-1698
    // TL;DR we need to strip '[' and ']' from IPv6 addresses if they come from a URL
    private final X509HostnameVerifier verifier = new org.apache.http.conn.ssl.StrictHostnameVerifier();

    private StrictHostnameVerifier() {}

    @Override
    public boolean verify(String host, SSLSession sslSession) {
        return verifier.verify(stripBracketsIfNecessary(host), sslSession);
    }

    @Override
    public void verify(String host, SSLSocket ssl) throws IOException {
        verifier.verify(stripBracketsIfNecessary(host), ssl);
    }

    @Override
    public void verify(String host, X509Certificate cert) throws SSLException {
        verifier.verify(stripBracketsIfNecessary(host), cert);
    }

    @Override
    public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
        verifier.verify(stripBracketsIfNecessary(host), cns, subjectAlts);
    }

    private String stripBracketsIfNecessary(String host) {
        if (host.startsWith("[") && host.endsWith("]")) {
            String newHost = host.substring(1, host.length() - 1);
            assert InetAddressUtils.isIPv6Address(newHost);
            return newHost;
        }
        return host;
    }
}
