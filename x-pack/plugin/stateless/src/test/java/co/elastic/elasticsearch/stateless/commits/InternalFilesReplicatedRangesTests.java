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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

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

    public void testWhenNoReplicatedContent() {
        assertThat(
            InternalFilesReplicatedRanges.EMPTY.getReplicatedPosition(randomNonNegativeLong(), randomNonNegativeInt()),
            equalTo(-1L)
        );
    }

    public void testWhenReplicatedContentIsNotPresent() {
        var position = new AtomicLong(0);
        var entries = randomList(0, 10, () -> {
            var p = position.get() + randomInt(1024 * 1024);
            var length = (short) (randomBoolean() ? randomFrom(1024, 16) : randomIntBetween(10, 1000));
            position.addAndGet(p + length);
            return new InternalFileReplicatedRange(p, length);
        });

        assertThat(
            InternalFilesReplicatedRanges.from(entries)
                .getReplicatedPosition(randomLongBetween(position.get(), Long.MAX_VALUE), randomNonNegativeInt()),
            equalTo(-1L)
        );
    }

    public void testWhenReplicatedContentIsPresent() {
        var position = new AtomicLong(0);
        var entries = randomList(1, 10, () -> {
            var p = position.get() + randomInt(1024 * 1024);
            var length = (short) (randomBoolean() ? randomFrom(1024, 16) : randomIntBetween(10, 1000));
            position.addAndGet(p + length);
            return new InternalFileReplicatedRange(p, length);
        });
        var target = randomFrom(entries);
        var targetReplicatedContentPosition = entries.stream()
            .filter(r -> r.position() < target.position())
            .mapToLong(InternalFileReplicatedRange::length)
            .sum();

        assertThat(
            InternalFilesReplicatedRanges.from(entries).getReplicatedPosition(target.position(), target.length()),
            equalTo(targetReplicatedContentPosition)
        );
    }

    public void testWhenRequestingSubRangeOfReplicatedContent() {
        var headers = InternalFilesReplicatedRanges.from(
            List.of(
                // file 1 header
                new InternalFileReplicatedRange(0, (short) 1024),
                // file 1 footer followed by file 2 header
                new InternalFileReplicatedRange(2048, (short) (16 + 1024)),
                // file 2 footer
                new InternalFileReplicatedRange(4096, (short) 16)
            )
        );
        // can find file 1 footer
        assertThat(headers.getReplicatedPosition(2048, 16), equalTo(1024L));
        // can find file 2 header
        assertThat(headers.getReplicatedPosition(2048 + 16, 1024), equalTo(1040L));
    }
}
