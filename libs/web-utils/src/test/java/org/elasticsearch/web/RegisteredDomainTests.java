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

import static org.elasticsearch.web.RegisteredDomain.getRegisteredDomainInfo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RegisteredDomainTests extends ESTestCase {

    public void testGetRegisteredDomain() {
        assertThat(
            getRegisteredDomainInfo("www.google.com"),
            is(new RegisteredDomain.DomainInfo("www.google.com", "google.com", "com", "www"))
        );
        assertThat(getRegisteredDomainInfo("google.com"), is(new RegisteredDomain.DomainInfo("google.com", "google.com", "com", null)));
        assertThat(getRegisteredDomainInfo(null), nullValue());
        assertThat(getRegisteredDomainInfo(""), nullValue());
        assertThat(getRegisteredDomainInfo(" "), nullValue());
        assertThat(getRegisteredDomainInfo("."), nullValue());
        assertThat(getRegisteredDomainInfo("$"), nullValue());
        assertThat(getRegisteredDomainInfo("foo.bar.baz"), nullValue());
        assertThat(
            getRegisteredDomainInfo("www.books.amazon.co.uk"),
            is(new RegisteredDomain.DomainInfo("www.books.amazon.co.uk", "amazon.co.uk", "co.uk", "www.books"))
        );
        // Verify "com" is returned as the eTLD, for that FQDN or subdomain
        assertThat(getRegisteredDomainInfo("com"), is(new RegisteredDomain.DomainInfo("com", null, "com", null)));
        assertThat(getRegisteredDomainInfo("example.com"), is(new RegisteredDomain.DomainInfo("example.com", "example.com", "com", null)));
        assertThat(
            getRegisteredDomainInfo("googleapis.com"),
            is(new RegisteredDomain.DomainInfo("googleapis.com", "googleapis.com", "com", null))
        );
        assertThat(
            getRegisteredDomainInfo("content-autofill.googleapis.com"),
            is(new RegisteredDomain.DomainInfo("content-autofill.googleapis.com", "googleapis.com", "com", "content-autofill"))
        );
        // Verify "ssl.fastly.net" is returned as the eTLD, for that FQDN or subdomain
        assertThat(
            getRegisteredDomainInfo("global.ssl.fastly.net"),
            is(new RegisteredDomain.DomainInfo("global.ssl.fastly.net", "global.ssl.fastly.net", "ssl.fastly.net", null))
        );
        assertThat(
            getRegisteredDomainInfo("1.www.global.ssl.fastly.net"),
            is(new RegisteredDomain.DomainInfo("1.www.global.ssl.fastly.net", "global.ssl.fastly.net", "ssl.fastly.net", "1.www"))
        );
    }
}
