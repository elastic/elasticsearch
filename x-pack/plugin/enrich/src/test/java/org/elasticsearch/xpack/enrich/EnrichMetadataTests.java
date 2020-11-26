/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;

public class EnrichMetadataTests extends AbstractSerializingTestCase<EnrichMetadata> {

    @Override
    protected EnrichMetadata doParseInstance(XContentParser parser) throws IOException {
        return EnrichMetadata.fromXContent(parser);
    }

    @Override
    protected EnrichMetadata createTestInstance() {
        return randomEnrichMetadata(randomFrom(XContentType.values()));
    }

    @Override
    protected EnrichMetadata createXContextTestInstance(XContentType xContentType) {
        return randomEnrichMetadata(xContentType);
    }

    private static EnrichMetadata randomEnrichMetadata(XContentType xContentType) {
        int numPolicies = randomIntBetween(8, 64);
        Map<String, EnrichPolicy> policies = new HashMap<>(numPolicies);
        for (int i = 0; i < numPolicies; i++) {
            EnrichPolicy policy = randomEnrichPolicy(xContentType);
            policies.put(randomAlphaOfLength(8), policy);
        }
        return new EnrichMetadata(policies);
    }

    @Override
    protected Writeable.Reader<EnrichMetadata> instanceReader() {
        return EnrichMetadata::new;
    }

    @Override
    protected void assertEqualInstances(EnrichMetadata expectedInstance, EnrichMetadata newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getPolicies().size(), equalTo(expectedInstance.getPolicies().size()));
        for (Map.Entry<String, EnrichPolicy> entry : newInstance.getPolicies().entrySet()) {
            EnrichPolicy actual = entry.getValue();
            EnrichPolicy expected = expectedInstance.getPolicies().get(entry.getKey());
            EnrichPolicyTests.assertEqualPolicies(expected, actual);
        }
    }
}
