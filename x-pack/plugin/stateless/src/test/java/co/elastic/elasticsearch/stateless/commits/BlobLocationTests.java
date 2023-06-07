package co.elastic.elasticsearch.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

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
        return switch (randomIntBetween(0, 4)) {
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
            case 2 -> new BlobLocation(
                instance.primaryTerm(),
                instance.blobName(),
                randomLongBetween(instance.blobLength(), Long.MAX_VALUE),
                instance.offset(),
                instance.fileLength()
            );
            case 3 -> blobLocation(
                instance.primaryTerm(),
                instance.blobName(),
                randomValueOtherThan(instance.offset(), () -> randomLongBetween(0, 100)),
                instance.fileLength()
            );
            case 4 -> blobLocation(
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
}
