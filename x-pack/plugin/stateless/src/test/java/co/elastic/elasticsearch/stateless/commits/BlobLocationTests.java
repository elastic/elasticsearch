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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
        return new BlobLocation(primaryTerm, blobName, offset, fileLength);
    }

    @Override
    protected BlobLocation mutateInstance(BlobLocation instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new BlobLocation(
                randomValueOtherThan(instance.primaryTerm(), () -> randomLongBetween(1, 10)),
                instance.blobName(),
                instance.offset(),
                instance.fileLength()
            );
            case 1 -> new BlobLocation(
                instance.primaryTerm(),
                randomValueOtherThan(instance.blobName(), () -> randomAlphaOfLength(10)),
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

    public void testNewFromXContentIgnoresBlobLength() throws IOException {
        final BlobLocation blobLocation = new BlobLocation(
            randomLongBetween(1, 10),
            randomAlphaOfLength(10),
            randomLongBetween(0, 100),
            randomLongBetween(100, 1000)
        );

        final BytesStreamOutput out = new BytesStreamOutput();
        try (var b = new XContentBuilder(XContentType.SMILE.xContent(), out)) {
            blobLocation.toXContent(b, ToXContent.EMPTY_PARAMS); // serialize with placeholder blobLength
            try (var p = XContentHelper.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b), XContentType.SMILE)) {
                final var deserialized = BlobLocation.fromXContent(p);
                assertThat(deserialized.primaryTerm(), equalTo(blobLocation.primaryTerm()));
                assertThat(deserialized.blobName(), equalTo(blobLocation.blobName()));
                assertThat(deserialized.offset(), equalTo(blobLocation.offset()));
                assertThat(deserialized.fileLength(), equalTo(blobLocation.fileLength()));
            }
        }
    }

    public void testWriteBlobLengthAndOldFromXContent() throws IOException {
        final BlobLocation blobLocation = new BlobLocation(
            randomLongBetween(1, 10),
            randomAlphaOfLength(10),
            randomLongBetween(0, 100),
            randomLongBetween(100, 1000)
        );

        // Define a parser that expects reading a blobLength field to simulate behaviours of nodes on older versions
        final ConstructingObjectParser<Boolean, Void> oldParser = new ConstructingObjectParser<>("blob_location", true, args -> {
            long primaryTerm = (long) args[0];
            String blobName = (String) args[1];
            long blobLength = (long) args[2];
            long offset = (long) args[3];
            long fileLength = (long) args[4];
            assertThat(new BlobLocation(primaryTerm, blobName, offset, fileLength), equalTo(blobLocation));
            assertThat(blobLength, equalTo(Long.MIN_VALUE));
            return true;
        });

        oldParser.declareLong(constructorArg(), new ParseField("primary_term"));
        oldParser.declareString(constructorArg(), new ParseField("blob_name"));
        oldParser.declareLong(constructorArg(), new ParseField("blob_length"));
        oldParser.declareLong(constructorArg(), new ParseField("offset"));
        oldParser.declareLong(constructorArg(), new ParseField("file_length"));

        final BytesStreamOutput out = new BytesStreamOutput();
        try (var b = new XContentBuilder(XContentType.SMILE.xContent(), out)) {
            blobLocation.toXContent(b, ToXContent.EMPTY_PARAMS); // serialize with placeholder blobLength
            try (var p = XContentHelper.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(b), XContentType.SMILE)) {
                assertThat(oldParser.parse(p, null), is(true));
            }
        }
    }
}
