/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;

/**
 * Test parsing of an eTLD from a FQDN. The list of eTLDs is maintained here:
 *   https://github.com/publicsuffix/list/blob/master/public_suffix_list.dat
 * <p>
 * Effective TLDs (eTLDs) are not the same as DNS TLDs. Uses for eTLDs are listed here:
 *   https://publicsuffix.org/learn/
 */
public class RegisteredDomainProcessorTests extends ESTestCase {

    public void testBasic() throws Exception {
        var processor = new RegisteredDomainProcessor(null, null, "input", "output", false);
        {
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("input", "www.google.co.uk"));
            processor.execute(document);
            assertThat(
                document.getSource(),
                is(
                    Map.ofEntries(
                        entry("input", "www.google.co.uk"),
                        entry(
                            "output",
                            Map.ofEntries(
                                entry("domain", "www.google.co.uk"),
                                entry("registered_domain", "google.co.uk"),
                                entry("top_level_domain", "co.uk"),
                                entry("subdomain", "www")
                            )
                        )
                    )
                )
            );
        }
        {
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("input", "example.com"));
            processor.execute(document);
            assertThat(
                document.getSource(),
                is(
                    Map.ofEntries(
                        entry("input", "example.com"),
                        entry(
                            "output",
                            Map.ofEntries(
                                entry("domain", "example.com"),
                                entry("registered_domain", "example.com"),
                                entry("top_level_domain", "com")
                            )
                        )
                    )
                )
            );
        }
        {
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("input", "com"));
            processor.execute(document);
            assertThat(
                document.getSource(),
                is(
                    Map.ofEntries(
                        entry("input", "com"),
                        entry(
                            "output",
                            Map.ofEntries(
                                entry("domain", "com"), //
                                entry("top_level_domain", "com")
                            )
                        )
                    )
                )
            );
        }
    }

    public void testUseRoot() throws Exception {
        var processor = new RegisteredDomainProcessor(null, null, "domain", "", false);
        IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("domain", "www.google.co.uk"));
        processor.execute(document);
        assertThat(
            document.getSource(),
            is(
                Map.ofEntries(
                    entry("domain", "www.google.co.uk"),
                    entry("registered_domain", "google.co.uk"),
                    entry("top_level_domain", "co.uk"),
                    entry("subdomain", "www")
                )
            )
        );
    }

    public void testError() throws Exception {
        var processor = new RegisteredDomainProcessor(null, null, "domain", "", false);

        {
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("domain", "foo.bar.baz"));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
            assertThat(e.getMessage(), is("unable to set domain information for document"));
            assertThat(document.getSource(), is(Map.of("domain", "foo.bar.baz")));
        }

        {
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of("domain", "$"));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
            assertThat(e.getMessage(), is("unable to set domain information for document"));
            assertThat(document.getSource(), is(Map.of("domain", "$")));
        }
    }

    public void testIgnoreMissing() throws Exception {
        {
            var processor = new RegisteredDomainProcessor(null, null, "domain", "", false);
            IngestDocument document = TestIngestDocument.withDefaultVersion(Map.of());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
            assertThat(e.getMessage(), is("field [domain] not present as part of path [domain]"));
            assertThat(document.getSource(), is(anEmptyMap()));
        }

        {
            var processor = new RegisteredDomainProcessor(null, null, "domain", "", true);
            IngestDocument document = TestIngestDocument.withDefaultVersion(Collections.singletonMap("domain", null));
            processor.execute(document);
            assertThat(document.getSource(), is(Collections.singletonMap("domain", null)));
        }
    }
}
