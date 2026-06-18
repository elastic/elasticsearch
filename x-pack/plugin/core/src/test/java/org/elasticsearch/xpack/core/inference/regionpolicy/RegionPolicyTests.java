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
import java.util.List;

public class RegionPolicyTests extends AbstractBWCSerializationTestCase<RegionPolicy> {

    private boolean ignoreUnknownFields = randomBoolean();

    @Override
    protected boolean supportsUnknownFields() {
        return ignoreUnknownFields;
    }

    @Override
    protected RegionPolicy mutateInstanceForVersion(RegionPolicy instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected RegionPolicy doParseInstance(XContentParser parser) throws IOException {
        return RegionPolicy.createParser(ignoreUnknownFields).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<RegionPolicy> instanceReader() {
        return RegionPolicy::new;
    }

    @Override
    protected RegionPolicy createTestInstance() {
        return createRandom();
    }

    public static RegionPolicy createRandom() {
        List<String> allowedGeos = null;
        List<CspRegion> allowedRegions = null;
        CspRegion fallbackRegion = null;

        boolean useGeos = randomBoolean();
        if (useGeos) {
            allowedGeos = randomList(10, () -> randomAlphaOfLength(10));
        } else {
            allowedRegions = randomList(10, () -> CspRegionTests.createRandom());
        }

        if (randomBoolean()) {
            fallbackRegion = CspRegionTests.createRandom();
        }
        return new RegionPolicy(allowedGeos, allowedRegions, fallbackRegion);
    }

    @Override
    protected RegionPolicy mutateInstance(RegionPolicy instance) throws IOException {
        List<String> allowedGeos = instance.allowedGeos();
        List<CspRegion> allowedRegions = instance.allowedRegions();
        CspRegion fallbackRegion = instance.fallbackRegion();
        switch (randomInt(2)) {
            case 0 -> allowedGeos = randomValueOtherThan(allowedGeos, () -> randomList(10, () -> randomAlphaOfLength(10)));
            case 1 -> allowedRegions = randomValueOtherThan(allowedRegions, () -> randomList(10, () -> CspRegionTests.createRandom()));
            case 2 -> fallbackRegion = randomValueOtherThan(fallbackRegion, CspRegionTests::createRandom);
        }
        return new RegionPolicy(allowedGeos, allowedRegions, fallbackRegion);
    }
}
