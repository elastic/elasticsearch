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

import static org.elasticsearch.xpack.stateless.commits.BlobFileTestUtils.randomBlobFile;

public class BlobLocationTests extends AbstractXContentSerializingTestCase<BlobLocation> {

    @Override
    protected Writeable.Reader<BlobLocation> instanceReader() {
        return BlobLocation::readFromTransport;
    }

    @Override
    protected BlobLocation createTestInstance() {
        return new BlobLocation(randomBlobFile(), randomLongBetween(0, 100), randomLongBetween(100, 1000));
    }

    @Override
    protected BlobLocation mutateInstance(BlobLocation instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new BlobLocation(
                randomValueOtherThan(instance.blobFile(), () -> randomBlobFile()),
                instance.offset(),
                instance.fileLength()
            );
            case 1 -> new BlobLocation(
                instance.blobFile(),
                randomValueOtherThan(instance.offset(), () -> randomLongBetween(0, 100)),
                instance.fileLength()
            );
            case 2 -> new BlobLocation(
                instance.blobFile(),
                instance.offset(),
                randomValueOtherThan(instance.fileLength(), () -> randomLongBetween(100, 1000))
            );
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testContains() {
        final var blobFile = randomBlobFile();
        final var outer = new BlobLocation(blobFile, 100, 100);
        assertTrue(outer.contains(new BlobLocation(blobFile, 100, 100)));
        assertTrue(outer.contains(new BlobLocation(blobFile, 150, 50)));
        assertFalse(outer.contains(new BlobLocation(blobFile, 150, 51)));
        assertFalse(outer.contains(new BlobLocation(randomValueOtherThan(blobFile, () -> randomBlobFile()), 100, 100)));
    }

    @Override
    protected BlobLocation doParseInstance(XContentParser parser) throws IOException {
        return BlobLocation.fromXContent(parser);
    }
}
