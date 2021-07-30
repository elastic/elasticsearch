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

import static org.hamcrest.Matchers.is;

public class WaitForSnapshotActionTests extends AbstractXContentTestCase<WaitForSnapshotAction> {

    @Override
    protected WaitForSnapshotAction doParseInstance(XContentParser parser) {
        return WaitForSnapshotAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    static WaitForSnapshotAction randomInstance() {
        return new WaitForSnapshotAction(randomAlphaOfLength(5));
    }

    @Override
    protected WaitForSnapshotAction createTestInstance() {
        return randomInstance();
    }

    public void testActionWithEmptyOrNullPolicy() {
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new WaitForSnapshotAction(""));
            assertThat(exception.getMessage(), is("policy name must be specified"));
        }

        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new WaitForSnapshotAction(null));
            assertThat(exception.getMessage(), is("policy name must be specified"));
        }
    }
}
