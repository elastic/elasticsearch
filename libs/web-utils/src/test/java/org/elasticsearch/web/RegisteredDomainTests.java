/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.web;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.web.RegisteredDomain.getRegisteredDomain;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RegisteredDomainTests extends ESTestCase {

    public void testGetRegisteredDomain() {
        assertThat(
            getRegisteredDomain("www.google.com"),
            is(new RegisteredDomain.DomainInfo("www.google.com", "google.com", "com", "www"))
        );
        assertThat(getRegisteredDomain("google.com"), is(new RegisteredDomain.DomainInfo("google.com", "google.com", "com", null)));
        assertThat(getRegisteredDomain(null), nullValue());
        assertThat(getRegisteredDomain(""), nullValue());
        assertThat(getRegisteredDomain(" "), nullValue());
        assertThat(getRegisteredDomain("."), nullValue());
        assertThat(getRegisteredDomain("$"), nullValue());
        assertThat(getRegisteredDomain("foo.bar.baz"), nullValue());
        assertThat(
            getRegisteredDomain("www.books.amazon.co.uk"),
            is(new RegisteredDomain.DomainInfo("www.books.amazon.co.uk", "amazon.co.uk", "co.uk", "www.books"))
        );
        // Verify "com" is returned as the eTLD, for that FQDN or subdomain
        assertThat(getRegisteredDomain("com"), is(new RegisteredDomain.DomainInfo("com", null, "com", null)));
        assertThat(getRegisteredDomain("example.com"), is(new RegisteredDomain.DomainInfo("example.com", "example.com", "com", null)));
        assertThat(
            getRegisteredDomain("googleapis.com"),
            is(new RegisteredDomain.DomainInfo("googleapis.com", "googleapis.com", "com", null))
        );
        assertThat(
            getRegisteredDomain("content-autofill.googleapis.com"),
            is(new RegisteredDomain.DomainInfo("content-autofill.googleapis.com", "googleapis.com", "com", "content-autofill"))
        );
        // Verify "ssl.fastly.net" is returned as the eTLD, for that FQDN or subdomain
        assertThat(
            getRegisteredDomain("global.ssl.fastly.net"),
            is(new RegisteredDomain.DomainInfo("global.ssl.fastly.net", "global.ssl.fastly.net", "ssl.fastly.net", null))
        );
        assertThat(
            getRegisteredDomain("1.www.global.ssl.fastly.net"),
            is(new RegisteredDomain.DomainInfo("1.www.global.ssl.fastly.net", "global.ssl.fastly.net", "ssl.fastly.net", "1.www"))
        );
    }
}
