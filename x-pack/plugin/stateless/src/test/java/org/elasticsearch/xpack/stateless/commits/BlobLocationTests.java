/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;

public class BlobLocationTests extends AbstractXContentSerializingTestCase<BlobLocation> {

    @Override
    protected Writeable.Reader<BlobLocation> instanceReader() {
        return BlobLocation::readFromTransport;
    }

    @Override
    protected BlobLocation createTestInstance() {
        return createBlobLocation(
            randomLongBetween(1, 10),
            randomLongBetween(1, 1000),
            randomLongBetween(0, 100),
            randomLongBetween(100, 1000)
        );
    }

    @Override
    protected BlobLocation mutateInstance(BlobLocation instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> createBlobLocation(
                randomValueOtherThan(instance.primaryTerm(), () -> randomLongBetween(1, 10)),
                instance.compoundFileGeneration(),
                instance.offset(),
                instance.fileLength()
            );
            case 1 -> createBlobLocation(
                instance.primaryTerm(),
                randomValueOtherThan(instance.compoundFileGeneration(), () -> randomLongBetween(1, 1000)),
                instance.offset(),
                instance.fileLength()
            );
            case 2 -> createBlobLocation(
                instance.primaryTerm(),
                instance.compoundFileGeneration(),
                randomValueOtherThan(instance.offset(), () -> randomLongBetween(0, 100)),
                instance.fileLength()
            );
            case 3 -> createBlobLocation(
                instance.primaryTerm(),
                instance.compoundFileGeneration(),
                instance.offset(),
                randomValueOtherThan(instance.fileLength(), () -> randomLongBetween(100, 1000))
            );
            default -> randomValueOtherThan(instance, this::createTestInstance);
        };
    }

    @Override
    protected BlobLocation doParseInstance(XContentParser parser) throws IOException {
        return BlobLocation.fromXContent(parser);
    }
}
