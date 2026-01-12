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

import co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.InternalFileReplicatedRange;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class InternalFilesReplicatedRangesTests extends AbstractXContentSerializingTestCase<InternalFileReplicatedRange> {

    @Override
    protected Writeable.Reader<InternalFileReplicatedRange> instanceReader() {
        return InternalFileReplicatedRange::fromStream;
    }

    @Override
    protected InternalFileReplicatedRange createTestInstance() {
        return new InternalFileReplicatedRange(randomNonNegativeLong(), (short) randomIntBetween(1, 1024));
    }

    @Override
    protected InternalFileReplicatedRange mutateInstance(InternalFileReplicatedRange instance) throws IOException {
        return switch (randomInt(1)) {
            case 0 -> new InternalFileReplicatedRange(
                randomValueOtherThan(instance.position(), ESTestCase::randomNonNegativeLong),
                instance.length()
            );
            case 1 -> new InternalFileReplicatedRange(
                instance.position(),
                randomValueOtherThan(instance.length(), () -> (short) randomIntBetween(1, 1024))
            );
            default -> throw new RuntimeException("unreachable");
        };
    }

    @Override
    protected InternalFileReplicatedRange doParseInstance(XContentParser parser) throws IOException {
        return InternalFileReplicatedRange.PARSER.parse(parser, null);
    }
}
