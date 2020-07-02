/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class EmptyConfigUpdateTests extends AbstractSerializingTestCase<EmptyConfigUpdate> {
    @Override
    protected EmptyConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return EmptyConfigUpdate.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<EmptyConfigUpdate> instanceReader() {
        return EmptyConfigUpdate::new;
    }

    @Override
    protected EmptyConfigUpdate createTestInstance() {
        return new EmptyConfigUpdate();
    }

    public void testToConfig() {
        expectThrows(UnsupportedOperationException.class, () -> new EmptyConfigUpdate().toConfig());
    }
}
