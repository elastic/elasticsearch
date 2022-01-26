/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutIndexTemplateRequestTests extends AbstractXContentTestCase<PutIndexTemplateRequest> {
    public void testValidateErrorMessage() throws Exception {
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest(null));
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest("test").name(null));
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test");
        ActionRequestValidationException withoutPattern = request.validate();
        assertThat(withoutPattern.getMessage(), containsString("index patterns are missing"));

        request.name("foo");
        ActionRequestValidationException withoutIndexPatterns = request.validate();
        assertThat(withoutIndexPatterns.validationErrors(), hasSize(1));
        assertThat(withoutIndexPatterns.getMessage(), containsString("index patterns are missing"));

        request.patterns(Collections.singletonList("test-*"));
        ActionRequestValidationException noError = request.validate();
        assertThat(noError, is(nullValue()));
    }

    @Override
    protected PutIndexTemplateRequest createTestInstance() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test");
        if (randomBoolean()) {
            request.version(randomInt());
        }
        if (randomBoolean()) {
            request.order(randomInt());
        }
        request.patterns(Arrays.asList(generateRandomStringArray(20, 100, false, false)));
        int numAlias = between(0, 5);
        for (int i = 0; i < numAlias; i++) {
            // some ASCII or Latin-1 control characters, especially newline, can lead to
            // problems with yaml parsers, that's why we filter them here (see #30911)
            Alias alias = new Alias(randomRealisticUnicodeOfLengthBetween(1, 10).replaceAll("\\p{Cc}", ""));
            if (randomBoolean()) {
                alias.indexRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            if (randomBoolean()) {
                alias.searchRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            request.alias(alias);
        }
        if (randomBoolean()) {
            try {
                request.mapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("field-" + randomInt())
                        .field("type", randomFrom("keyword", "text"))
                        .endObject()
                        .endObject()
                        .endObject()
                );
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (randomBoolean()) {
            request.settings(Settings.builder().put("setting1", randomLong()).put("setting2", randomTimeValue()).build());
        }
        return request;
    }

    @Override
    protected PutIndexTemplateRequest doParseInstance(XContentParser parser) throws IOException {
        return new PutIndexTemplateRequest("test").source(parser.map());
    }

    @Override
    protected void assertEqualInstances(PutIndexTemplateRequest expected, PutIndexTemplateRequest actual) {
        assertNotSame(expected, actual);
        assertThat(actual.version(), equalTo(expected.version()));
        assertThat(actual.order(), equalTo(expected.order()));
        assertThat(actual.patterns(), equalTo(expected.patterns()));
        assertThat(actual.aliases(), equalTo(expected.aliases()));
        assertThat(actual.mappings(), equalTo(expected.mappings()));
        assertThat(actual.settings(), equalTo(expected.settings()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
