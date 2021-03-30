/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GapPolicyTests extends AbstractWriteableEnumTestCase {

    public GapPolicyTests() {
        super(BucketHelpers.GapPolicy::readFrom);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(BucketHelpers.GapPolicy.INSERT_ZEROS.ordinal(), equalTo(0));
        assertThat(BucketHelpers.GapPolicy.SKIP.ordinal(), equalTo(1));
    }

    @Override
    public void testFromString() {
        assertThat(BucketHelpers.GapPolicy.parse("insert_zeros", null), equalTo(BucketHelpers.GapPolicy.INSERT_ZEROS));
        assertThat(BucketHelpers.GapPolicy.parse("skip", null), equalTo(BucketHelpers.GapPolicy.SKIP));
        ParsingException e = expectThrows(ParsingException.class, () -> BucketHelpers.GapPolicy.parse("does_not_exist", null));
        assertThat(e.getMessage(),
            equalTo("Invalid gap policy: [does_not_exist], accepted values: [insert_zeros, skip]"));
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, BucketHelpers.GapPolicy.INSERT_ZEROS);
        assertReadFromStream(1, BucketHelpers.GapPolicy.SKIP);
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(BucketHelpers.GapPolicy.INSERT_ZEROS, 0);
        assertWriteToStream(BucketHelpers.GapPolicy.SKIP, 1);
    }
}
