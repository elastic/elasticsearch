/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

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
        return new ForceMergeAction(randomIntBetween(1, 100));
    }

    public void testMissingMaxNumSegments() throws IOException {
        BytesReference emptyObject = BytesReference.bytes(JsonXContent.contentBuilder().startObject().endObject());
        XContentParser parser = XContentHelper.createParser(null, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            emptyObject, XContentType.JSON);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ForceMergeAction.parse(parser));
        assertThat(e.getMessage(), equalTo("Required [max_num_segments]"));
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(-10, 0)));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }
}
