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

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.BLOB_LOCATION_WITHOUT_BLOB_LENGTH;

public class BlobLocationTests extends AbstractXContentSerializingTestCase<BlobLocation> {

    @Override
    protected Writeable.Reader<BlobLocation> instanceReader() {
        return BlobLocation::readFromTransport;
    }

    @Override
    protected BlobLocation createTestInstance() {
        return blobLocation(randomLongBetween(1, 10), randomAlphaOfLength(10), randomLongBetween(0, 100), randomLongBetween(100, 1000));
    }

    private static BlobLocation blobLocation(long primaryTerm, String blobName, long offset, long fileLength) {
        return new BlobLocation(primaryTerm, blobName, offset + fileLength, offset, fileLength);
    }

    @Override
    protected BlobLocation mutateInstance(BlobLocation instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new BlobLocation(
                randomValueOtherThan(instance.primaryTerm(), () -> randomLongBetween(1, 10)),
                instance.blobName(),
                instance.blobLength(),
                instance.offset(),
                instance.fileLength()
            );
            case 1 -> new BlobLocation(
                instance.primaryTerm(),
                randomValueOtherThan(instance.blobName(), () -> randomAlphaOfLength(10)),
                instance.blobLength(),
                instance.offset(),
                instance.fileLength()
            );
            case 2 -> blobLocation(
                instance.primaryTerm(),
                instance.blobName(),
                randomValueOtherThan(instance.offset(), () -> randomLongBetween(0, 100)),
                instance.fileLength()
            );
            case 3 -> blobLocation(
                instance.primaryTerm(),
                instance.blobName(),
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

    public void testWireSerializationBwc() throws IOException {
        final TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(BLOB_LOCATION_WITHOUT_BLOB_LENGTH);
        final BlobLocation blobLocation = createTestInstance();
        assertSerialization(blobLocation, previousVersion); // this exercises the equalTo and hashCode logic for excluding blobLength
    }
}
