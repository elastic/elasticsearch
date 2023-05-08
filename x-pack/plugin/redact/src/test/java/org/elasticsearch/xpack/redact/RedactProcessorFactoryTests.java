/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.redact;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

public class RedactProcessorFactoryTests extends ESTestCase {

    private static XPackLicenseState mockLicenseState() {
        MockLicenseState licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(RedactProcessor.REDACT_PROCESSOR_FEATURE)).thenReturn(true);
        return licenseState;
    }

    private static XPackLicenseState mockNotAllowedLicenseState() {
        MockLicenseState licenseState = TestUtils.newMockLicenceState();
        when(licenseState.isAllowed(RedactProcessor.REDACT_PROCESSOR_FEATURE)).thenReturn(false);
        return licenseState;
    }

    public void testPatternNotSet() {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of());
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), containsString("List of patterns must not be empty"));
    }

    public void testCreateWithCustomPatterns() throws Exception {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));
        RedactProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getGroks(), not(empty()));
        assertThat(processor.getGroks().get(0).match("foo!"), equalTo(true));
    }

    public void testConfigKeysRemoved() throws Exception {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));
        config.put("ignore_missing", true);
        config.put("extra", "unused");

        factory.create(null, null, null, config);
        assertThat(config.entrySet(), hasSize(1));
        assertEquals("unused", config.get("extra"));
    }

    public void testSkipIfUnlicensed() throws Exception {
        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "_field");
            config.put("patterns", List.of("%{MY_PATTERN:name}!"));
            config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));

            // the default value for skip_if_unlicensed is true, and so the license state doesn't matter
            XPackLicenseState licenseState = randomBoolean() ? mockLicenseState() : mockNotAllowedLicenseState();
            RedactProcessor.Factory factory = new RedactProcessor.Factory(licenseState, MatcherWatchdog.noop());
            RedactProcessor processor = factory.create(null, null, null, config);
            processor.extraValidation();
            assertThat(processor.getSkipIfUnlicensed(), equalTo(true));
        }

        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "_field");
            config.put("patterns", List.of("%{MY_PATTERN:name}!"));
            config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));

            // but it can be set to false if you wish, in which case the license check must pass
            config.put("skip_if_unlicensed", false);
            RedactProcessor.Factory factory = new RedactProcessor.Factory(mockLicenseState(), MatcherWatchdog.noop());
            RedactProcessor processor = factory.create(null, null, null, config);
            processor.extraValidation();
            assertThat(processor.getSkipIfUnlicensed(), equalTo(false));
        }

        {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "_field");
            config.put("patterns", List.of("%{MY_PATTERN:name}!"));
            config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));

            // if skip_if_unlicensed is false, then the license must allow for redact to be used in order to pass the extra validation
            config.put("skip_if_unlicensed", false);
            RedactProcessor.Factory factory = new RedactProcessor.Factory(mockNotAllowedLicenseState(), MatcherWatchdog.noop());
            RedactProcessor processor = factory.create(null, null, null, config);
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> processor.extraValidation());
            assertThat(e.getMessage(), containsString("[skip_if_unlicensed] current license is non-compliant for [redact_processor]"));
        }
    }

}
