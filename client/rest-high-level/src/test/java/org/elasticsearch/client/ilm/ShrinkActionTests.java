/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ShrinkActionTests extends AbstractXContentTestCase<ShrinkAction> {

    @Override
    protected ShrinkAction doParseInstance(XContentParser parser) throws IOException {
        return ShrinkAction.parse(parser);
    }

    @Override
    protected ShrinkAction createTestInstance() {
        return randomInstance();
    }

    static ShrinkAction randomInstance() {
        if (randomBoolean()) {
            return new ShrinkAction(randomIntBetween(1, 100), null);
        } else {
            return new ShrinkAction(null, new ByteSizeValue(randomIntBetween(1, 100)));
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0), null));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }

    public void testMaxPrimaryShardSize() {
        ByteSizeValue maxPrimaryShardSize1 = new ByteSizeValue(10);
        Exception e1 = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(1, 100), maxPrimaryShardSize1));
        assertThat(e1.getMessage(), equalTo("Cannot set both [number_of_shards] and [max_primary_shard_size]"));

        ByteSizeValue maxPrimaryShardSize2 = new ByteSizeValue(0);
        Exception e2 = expectThrows(Exception.class, () -> new org.elasticsearch.client.ilm.ShrinkAction(null, maxPrimaryShardSize2));
        assertThat(e2.getMessage(), equalTo("[max_primary_shard_size] must be greater than 0"));
    }
}
