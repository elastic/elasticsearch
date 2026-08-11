/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.regionpolicy;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class CspRegionTests extends AbstractBWCSerializationTestCase<CspRegion> {

    private boolean ignoreUnknownFields = randomBoolean();

    @Override
    protected boolean supportsUnknownFields() {
        return ignoreUnknownFields;
    }

    @Override
    protected CspRegion mutateInstanceForVersion(CspRegion instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected CspRegion doParseInstance(XContentParser parser) throws IOException {
        var xParser = ignoreUnknownFields ? CspRegion.LENIENT_PARSER : CspRegion.STRICT_PARSER;
        return xParser.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<CspRegion> instanceReader() {
        return CspRegion::new;
    }

    @Override
    protected CspRegion createTestInstance() {
        return createRandom();
    }

    @Override
    protected CspRegion mutateInstance(CspRegion instance) throws IOException {
        String csp = instance.csp();
        String region = instance.region();
        switch (randomInt(1)) {
            case 0 -> csp = randomValueOtherThan(csp, () -> randomAlphaOfLength(3));
            case 1 -> region = randomValueOtherThan(region, () -> randomAlphaOfLength(10));
            default -> throw new IllegalStateException("Illegal randomization switch case");
        }
        return new CspRegion(csp, region);
    }

    public static CspRegion createRandom() {
        return new CspRegion(randomAlphaOfLength(3), randomAlphaOfLength(10));
    }
}
