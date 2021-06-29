/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SearchableSnapshotActionTests extends AbstractXContentTestCase<SearchableSnapshotAction> {

    @Override
    protected SearchableSnapshotAction doParseInstance(XContentParser parser) throws IOException {
        return SearchableSnapshotAction.parse(parser);
    }

    @Override
    protected SearchableSnapshotAction createTestInstance() {
        return randomInstance();
    }

    static SearchableSnapshotAction randomInstance() {
        return new SearchableSnapshotAction(randomAlphaOfLengthBetween(5, 10), randomBoolean());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testEmptyOrNullRepository() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SearchableSnapshotAction(""));
            assertThat(e.getMessage(), equalTo("the snapshot repository must be specified"));
        }
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SearchableSnapshotAction(null));
            assertThat(e.getMessage(), equalTo("the snapshot repository must be specified"));
        }
    }
}
