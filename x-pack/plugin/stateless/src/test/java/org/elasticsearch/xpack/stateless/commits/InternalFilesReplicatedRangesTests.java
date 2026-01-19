/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges.InternalFileReplicatedRange;

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
