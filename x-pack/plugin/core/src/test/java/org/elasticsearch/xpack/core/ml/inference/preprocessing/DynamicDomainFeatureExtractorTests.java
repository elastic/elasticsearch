/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;

public class DynamicDomainFeatureExtractorTests extends PreProcessingTests<DynamicDomainFeatureExtractor> {

    @Override
    protected DynamicDomainFeatureExtractor doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            DynamicDomainFeatureExtractor.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            DynamicDomainFeatureExtractor.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected DynamicDomainFeatureExtractor createTestInstance() {
        return createRandom();
    }

    public static DynamicDomainFeatureExtractor createRandom() {
        return new DynamicDomainFeatureExtractor(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : Stream.generate(() -> randomAlphaOfLength(10))
                .limit(randomIntBetween(0, 10))
                .collect(Collectors.toSet()));
    }

    @Override
    protected Writeable.Reader<DynamicDomainFeatureExtractor> instanceReader() {
        return DynamicDomainFeatureExtractor::new;
    }

    public void testProcessWithFieldPresent_WhenRegisteredDomainIsMissing() {
        Map<String, Object> fieldValues = randomFieldValues();
        fieldValues.put("domainName", "elastic");
        DynamicDomainFeatureExtractor encoding = new DynamicDomainFeatureExtractor("domainName",
            "registeredDomain",
            "subdomain",
            "tld",
            "f",
            null);
        encoding.process(fieldValues);

        assertThat(fieldValues, hasEntry("f.sld", "elastic"));
        assertThat(fieldValues, hasEntry("f.tld", ""));
    }

    public void testProcessWithFieldPresent_WithRegisteredDynamicDomain() {
        Map<String, Object> fieldValues = randomFieldValues();
        fieldValues.put("registeredDomain", "github.io");
        fieldValues.put("tld", "io");
        fieldValues.put("subdomain", "elastic");
        DynamicDomainFeatureExtractor encoding = new DynamicDomainFeatureExtractor("domainName",
            "registeredDomain",
            "subdomain",
            "tld",
            "f",
            Collections.singleton("github.io"));
        encoding.process(fieldValues);

        assertThat(fieldValues, hasEntry("f.sld", "elastic"));
        assertThat(fieldValues, hasEntry("f.tld", "github.io"));
    }

    public void testProcessWithFieldPresent_WithRegisteredDomainAndTld() {
        Map<String, Object> fieldValues = randomFieldValues();
        fieldValues.put("domainName", "github");
        fieldValues.put("registeredDomain", "github.io");
        fieldValues.put("tld", "io");
        DynamicDomainFeatureExtractor encoding = new DynamicDomainFeatureExtractor("domainName",
            "registeredDomain",
            "subdomain",
            "tld",
            "f",
            null);
        encoding.process(fieldValues);

        assertThat(fieldValues, hasEntry("f.sld", "github"));
        assertThat(fieldValues, hasEntry("f.tld", "io"));
    }

    public void testInputOutputFields() {
        DynamicDomainFeatureExtractor encoding = new DynamicDomainFeatureExtractor("domainName",
            "registeredDomain",
            "subdomain",
            "tld",
            "f",
            null);
        assertThat(encoding.inputFields(), contains("domainName", "registeredDomain", "subdomain", "tld"));
        assertThat(encoding.outputFields(), contains("f.tld", "f.sld"));
    }

}
