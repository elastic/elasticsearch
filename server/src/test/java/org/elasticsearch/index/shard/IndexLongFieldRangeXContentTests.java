/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.checkForSameInstances;
import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomRange;
import static org.hamcrest.Matchers.sameInstance;

public class IndexLongFieldRangeXContentTests extends AbstractXContentTestCase<IndexLongFieldRange> {
    @Override
    protected IndexLongFieldRange createTestInstance() {
        return randomRange();
    }

    @Override
    protected IndexLongFieldRange doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), sameInstance(XContentParser.Token.START_OBJECT));
        return IndexLongFieldRange.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(IndexLongFieldRange expectedInstance, IndexLongFieldRange newInstance) {
        if (checkForSameInstances(expectedInstance, newInstance) == false) {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }
}
