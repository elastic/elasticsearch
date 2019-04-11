/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;

public class EnrichPolicyTests extends AbstractSerializingTestCase<EnrichPolicy> {

    @Override
    protected EnrichPolicy doParseInstance(XContentParser parser) throws IOException {
        return EnrichPolicy.PARSER.parse(parser, null);
    }

    @Override
    protected EnrichPolicy createTestInstance() {
        return randomEnrichPolicy();
    }

    static EnrichPolicy randomEnrichPolicy() {
        return new EnrichPolicy(
            randomFrom(EnrichPolicy.Type.values()),
            new TimeValue(randomNonNegativeLong()),
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            Arrays.asList(generateRandomStringArray(8, 4, false, false))
        );
    }

    @Override
    protected Writeable.Reader<EnrichPolicy> instanceReader() {
        return EnrichPolicy::new;
    }
}
