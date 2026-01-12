/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static co.elastic.elasticsearch.stateless.commits.BlobLocationTestUtils.createBlobLocation;

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
