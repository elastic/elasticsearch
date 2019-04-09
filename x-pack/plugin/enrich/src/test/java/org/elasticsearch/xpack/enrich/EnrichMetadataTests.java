/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EnrichMetadataTests extends AbstractSerializingTestCase<EnrichMetadata> {

    @Override
    protected EnrichMetadata doParseInstance(XContentParser parser) throws IOException {
        return EnrichMetadata.fromXContent(parser);
    }

    @Override
    protected EnrichMetadata createTestInstance() {
        int numPolicies = randomIntBetween(8, 64);
        Map<String, EnrichMetadata.Policy> policies = new HashMap<>(numPolicies);
        for (int i = 0; i < numPolicies; i++) {
            policies.put(randomAlphaOfLength(8), new EnrichMetadata.Policy(
                randomFrom(EnrichMetadata.Policy.Type.values()),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                Arrays.asList(generateRandomStringArray(8, 4, false, false))
            ));
        }
        return new EnrichMetadata(policies);
    }

    @Override
    protected Writeable.Reader<EnrichMetadata> instanceReader() {
        return EnrichMetadata::new;
    }
}
