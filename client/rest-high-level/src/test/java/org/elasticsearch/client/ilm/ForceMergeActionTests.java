/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import static org.hamcrest.Matchers.equalTo;

public class ForceMergeActionTests extends AbstractXContentTestCase<ForceMergeAction> {

    @Override
    protected ForceMergeAction doParseInstance(XContentParser parser) {
        return ForceMergeAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ForceMergeAction createTestInstance() {
        return randomInstance();
    }

    static ForceMergeAction randomInstance() {
        Integer maxNumSegments = null;
        boolean onlyExpungeDeletes = randomBoolean();
        if (onlyExpungeDeletes == false) {
            maxNumSegments = randomIntBetween(1, 100);
        }
        return new ForceMergeAction(maxNumSegments, createRandomCompressionSettings(), onlyExpungeDeletes);
    }

    static String createRandomCompressionSettings() {
        if (randomBoolean()) {
            return null;
        }
        return CodecService.BEST_COMPRESSION_CODEC;
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(-10, 0), null, false));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }

    public void testInvalidCodec() {
        Exception r = expectThrows(
            IllegalArgumentException.class,
            () -> new ForceMergeAction(randomIntBetween(1, 10), "DummyCompressingStoredFields", false)
        );
        assertThat(r.getMessage(), equalTo("unknown index codec: [DummyCompressingStoredFields]"));
    }

    public void testInvalidOnyExpungeDeletes() {
        Exception exception = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(1, 10), null, true));
        assertThat(
            exception.getMessage(),
            equalTo(
                "cannot set [max_num_segments] and [only_expunge_deletes] at the same time,"
                    + " those two parameters are mutually exclusive"
            )
        );

        exception = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(null, null, false));
        assertThat(exception.getMessage(), equalTo("Either [max_num_segments] or [only_expunge_deletes] must be set"));
    }
}
