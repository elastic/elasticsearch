/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.ssl.X509Field.SAN_DNS;
import static org.elasticsearch.common.ssl.X509Field.SAN_OTHERNAME_COMMONNAME;
import static org.hamcrest.Matchers.containsString;

public class X509FieldTests extends ESTestCase {

    public void testParseForRestrictedTrust() {
        assertEquals(SAN_OTHERNAME_COMMONNAME, X509Field.parseForRestrictedTrust("subjectAltName.otherName.commonName"));
        assertEquals(SAN_DNS, X509Field.parseForRestrictedTrust("subjectAltName.dnsName"));
        SslConfigException exception = expectThrows(SslConfigException.class, () -> X509Field.parseForRestrictedTrust("foo.bar"));
        assertThat(exception.getMessage(), containsString("foo.bar"));
        assertThat(exception.getMessage(), containsString("is not a supported x509 field for trust restrictions"));
        assertThat(exception.getMessage(), containsString(SAN_OTHERNAME_COMMONNAME.toString()));
        assertThat(exception.getMessage(), containsString(SAN_DNS.toString()));
    }
}
