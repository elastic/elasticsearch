/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.audit;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class InferenceAuditEventDocTests extends AbstractXContentTestCase<InferenceAuditEventDoc> {

    private static Instant randomInstantStatic() {
        return Instant.ofEpochSecond(randomLongBetween(0, 3_000_000_000L), randomLongBetween(0, 999_999_999));
    }

    @Override
    protected InferenceAuditEventDoc doParseInstance(XContentParser parser) throws IOException {
        return InferenceAuditEventDoc.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected InferenceAuditEventDoc createTestInstance() {
        return createRandom();
    }

    public static InferenceAuditEventDoc createRandom() {
        Map<String, Object> resource = randomBoolean() ? null : Map.of("region_policy", Map.of("allowed_geos", List.of("us", "eu")));
        return new InferenceAuditEventDoc(
            randomInstantStatic(),
            randomFrom(InferenceAuditEventDoc.Action.values()),
            randomFrom(InferenceAuditEventDoc.ResourceType.values()),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            resource
        );
    }
}
