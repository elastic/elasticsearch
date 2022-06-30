/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test parsing of an eTLD from a FQDN. The list of eTLDs is maintained here:
 *   https://github.com/publicsuffix/list/blob/master/public_suffix_list.dat
 *
 * Effective TLDs (eTLS) are not the same as DNS TLDs. Uses for eTLDs are listed here.
 *   https://publicsuffix.org/learn/
 */
public class RegisteredDomainProcessorTests extends ESTestCase {
    private Map<String, Object> buildEvent(String domain) {
        return new HashMap<>() {
            {
                put("domain", domain);
            }
        };
    }

    public void testBasic() throws Exception {
        testRegisteredDomainProcessor(buildEvent("www.google.com"), "www.google.com", "google.com", "com", "www");
        testRegisteredDomainProcessor(buildEvent("google.com"), "google.com", "google.com", "com", null);
        testRegisteredDomainProcessor(buildEvent(""), null, null, null, null);
        testRegisteredDomainProcessor(buildEvent("."), null, null, null, null);
        testRegisteredDomainProcessor(buildEvent("$"), null, null, null, null);
        testRegisteredDomainProcessor(buildEvent("foo.bar.baz"), null, null, null, null);
        testRegisteredDomainProcessor(buildEvent("www.books.amazon.co.uk"), "www.books.amazon.co.uk", "amazon.co.uk", "co.uk", "www.books");
        // Verify "com" is returned as the eTLD, for that FQDN or subdomain
        testRegisteredDomainProcessor(buildEvent("com"), "com", null, "com", null);
        testRegisteredDomainProcessor(buildEvent("example.com"), "example.com", "example.com", "com", null);
        testRegisteredDomainProcessor(buildEvent("googleapis.com"), "googleapis.com", "googleapis.com", "com", null);
        testRegisteredDomainProcessor(
            buildEvent("content-autofill.googleapis.com"),
            "content-autofill.googleapis.com",
            "googleapis.com",
            "com",
            "content-autofill"
        );
        // Verify "ssl.fastly.net" is returned as the eTLD, for that FQDN or subdomain
        testRegisteredDomainProcessor(
            buildEvent("global.ssl.fastly.net"),
            "global.ssl.fastly.net",
            "global.ssl.fastly.net",
            "ssl.fastly.net",
            null
        );
        testRegisteredDomainProcessor(
            buildEvent("1.www.global.ssl.fastly.net"),
            "1.www.global.ssl.fastly.net",
            "global.ssl.fastly.net",
            "ssl.fastly.net",
            "1.www"
        );
    }

    public void testUseRoot() throws Exception {
        Map<String, Object> source = buildEvent("www.google.co.uk");

        String domainField = "domain";
        String registeredDomainField = "registered_domain";
        String topLevelDomainField = "top_level_domain";
        String subdomainField = "subdomain";

        var processor = new RegisteredDomainProcessor(null, null, "domain", "", false);

        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        String domain = output.getFieldValue(domainField, String.class);
        assertThat(domain, equalTo("www.google.co.uk"));
        String registeredDomain = output.getFieldValue(registeredDomainField, String.class);
        assertThat(registeredDomain, equalTo("google.co.uk"));
        String eTLD = output.getFieldValue(topLevelDomainField, String.class);
        assertThat(eTLD, equalTo("co.uk"));
        String subdomain = output.getFieldValue(subdomainField, String.class);
        assertThat(subdomain, equalTo("www"));
    }

    public void testError() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testRegisteredDomainProcessor(buildEvent("foo.bar.baz"), null, null, null, null, false)
        );
        assertThat(e.getMessage(), containsString("unable to set domain information for document"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> testRegisteredDomainProcessor(buildEvent("$"), null, null, null, null, false)
        );
        assertThat(e.getMessage(), containsString("unable to set domain information for document"));
    }

    private void testRegisteredDomainProcessor(
        Map<String, Object> source,
        String expectedDomain,
        String expectedRegisteredDomain,
        String expectedETLD,
        String expectedSubdomain
    ) throws Exception {
        testRegisteredDomainProcessor(source, expectedDomain, expectedRegisteredDomain, expectedETLD, expectedSubdomain, true);
    }

    private void testRegisteredDomainProcessor(
        Map<String, Object> source,
        String expectedDomain,
        String expectedRegisteredDomain,
        String expectedETLD,
        String expectedSubdomain,
        boolean ignoreMissing
    ) throws Exception {
        String domainField = "url.domain";
        String registeredDomainField = "url.registered_domain";
        String topLevelDomainField = "url.top_level_domain";
        String subdomainField = "url.subdomain";

        var processor = new RegisteredDomainProcessor(null, null, "domain", "url", ignoreMissing);

        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        String domain = output.getFieldValue(domainField, String.class, expectedDomain == null);
        assertThat(domain, equalTo(expectedDomain));
        String registeredDomain = output.getFieldValue(registeredDomainField, String.class, expectedRegisteredDomain == null);
        assertThat(registeredDomain, equalTo(expectedRegisteredDomain));
        String eTLD = output.getFieldValue(topLevelDomainField, String.class, expectedETLD == null);
        assertThat(eTLD, equalTo(expectedETLD));
        String subdomain = output.getFieldValue(subdomainField, String.class, expectedSubdomain == null);
        assertThat(subdomain, equalTo(expectedSubdomain));
    }
}
