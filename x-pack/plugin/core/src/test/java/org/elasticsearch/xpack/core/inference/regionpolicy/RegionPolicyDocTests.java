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
import java.time.Instant;
import java.util.function.Supplier;

public class RegionPolicyDocTests extends AbstractBWCSerializationTestCase<RegionPolicyDoc> {

    private boolean ignoreUnknownFields = randomBoolean();

    @Override
    protected boolean supportsUnknownFields() {
        return ignoreUnknownFields;
    }

    @Override
    protected RegionPolicyDoc mutateInstanceForVersion(RegionPolicyDoc instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected RegionPolicyDoc doParseInstance(XContentParser parser) throws IOException {
        return RegionPolicyDoc.createParser(ignoreUnknownFields).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<RegionPolicyDoc> instanceReader() {
        return RegionPolicyDoc::new;
    }

    @Override
    protected RegionPolicyDoc createTestInstance() {
        return createRandom(this::randomInstant);
    }

    public static RegionPolicyDoc createRandom(Supplier<Instant> randomInstantSupplier) {
        RegionPolicy regionPolicy = RegionPolicyTests.createRandom();
        Instant createdAt = randomInstantSupplier.get();
        String createdBy = randomBoolean() ? null : randomAlphaOfLength(10);
        Instant updatedAt = randomBoolean() ? null : randomInstantSupplier.get();
        String updatedBy = randomBoolean() ? null : randomAlphaOfLength(10);
        return new RegionPolicyDoc(regionPolicy, createdAt, createdBy, updatedAt, updatedBy);
    }

    @Override
    protected RegionPolicyDoc mutateInstance(RegionPolicyDoc instance) throws IOException {
        RegionPolicy regionPolicy = instance.regionPolicy();
        Instant createdAt = instance.createdAt();
        String createdBy = instance.createdBy();
        Instant updatedAt = instance.updatedAt();
        String updatedBy = instance.updatedBy();
        switch (randomInt(4)) {
            case 0 -> regionPolicy = randomValueOtherThan(regionPolicy, RegionPolicyTests::createRandom);
            case 1 -> createdAt = randomValueOtherThan(createdAt, () -> randomInstant());
            case 2 -> createdBy = randomValueOtherThan(createdBy, () -> randomBoolean() ? null : randomAlphaOfLength(10));
            case 3 -> updatedAt = randomValueOtherThan(updatedAt, () -> randomBoolean() ? null : randomInstant());
            case 4 -> updatedBy = randomValueOtherThan(updatedBy, () -> randomBoolean() ? null : randomAlphaOfLength(10));
            default -> throw new IllegalStateException("Illegal randomisation branch");
        }
        return new RegionPolicyDoc(regionPolicy, createdAt, createdBy, updatedAt, updatedBy);
    }
}
