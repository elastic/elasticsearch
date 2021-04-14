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
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;

public class MigrateActionTests extends AbstractXContentTestCase<MigrateAction> {

    @Override
    protected MigrateAction doParseInstance(XContentParser parser) throws IOException {
        return MigrateAction.parse(parser);
    }

    @Override
    protected MigrateAction createTestInstance() {
        return randomInstance();
    }

    static MigrateAction randomInstance() {
        return new MigrateAction(randomBoolean());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testEqualsHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(),
            m -> new MigrateAction(m.isEnabled()),
            m -> new MigrateAction(m.isEnabled() == false));
    }
}
