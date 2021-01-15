/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RegisteredDomainProcessorTests extends ESTestCase {
    private Map<String, Object> buildEvent(String domain) {
        return new HashMap<>() {
            {
                put("domain", domain);
            }
        };
    }

    public void testBasic() throws Exception {
        testRegisteredDomainProcessor(buildEvent("www.google.com"), "google.com", "com", "www");
        testRegisteredDomainProcessor(buildEvent(""), null, null, null);
        testRegisteredDomainProcessor(buildEvent("."), null, null, null);
        testRegisteredDomainProcessor(buildEvent("$"), null, null, null);
        testRegisteredDomainProcessor(buildEvent("foo.bar.baz"), null, null, null);
        testRegisteredDomainProcessor(buildEvent("1.www.global.ssl.fastly.net"), "www.global.ssl.fastly.net", "global.ssl.fastly.net", "1");
    }

    public void testError() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testRegisteredDomainProcessor(buildEvent("foo.bar.baz"), null, null, null, false)
        );
        assertThat(e.getMessage(), containsString("unable to set domain information for document"));
        e = expectThrows(IllegalArgumentException.class, () -> testRegisteredDomainProcessor(buildEvent("$"), null, null, null, false));
        assertThat(e.getMessage(), containsString("unable to set domain information for document"));
    }

    private void testRegisteredDomainProcessor(
        Map<String, Object> source,
        String expectedRegisteredDomain,
        String expectedETLD,
        String expectedSubdomain
    ) throws Exception {
        testRegisteredDomainProcessor(source, expectedRegisteredDomain, expectedETLD, expectedSubdomain, true);
    }

    private void testRegisteredDomainProcessor(
        Map<String, Object> source,
        String expectedRegisteredDomain,
        String expectedETLD,
        String expectedSubdomain,
        boolean ignoreMissing
    ) throws Exception {
        String registeredDomainField = "registered_domain";
        String topLevelDomainField = "top_level_domain";
        String subdomainField = "subdomain";

        var processor = new RegisteredDomainProcessor(
            null,
            null,
            "domain",
            registeredDomainField,
            topLevelDomainField,
            subdomainField,
            ignoreMissing
        );

        IngestDocument input = new IngestDocument(source, Map.of());
        IngestDocument output = processor.execute(input);

        String publicSuffix = output.getFieldValue(registeredDomainField, String.class, expectedRegisteredDomain == null);
        assertThat(publicSuffix, equalTo(expectedRegisteredDomain));
        String eTLD = output.getFieldValue(topLevelDomainField, String.class, expectedETLD == null);
        assertThat(eTLD, equalTo(expectedETLD));
        String subdomain = output.getFieldValue(subdomainField, String.class, expectedSubdomain == null);
        assertThat(subdomain, equalTo(expectedSubdomain));
    }
}
