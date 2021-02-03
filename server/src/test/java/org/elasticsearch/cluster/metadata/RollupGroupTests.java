/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RollupGroupTests extends AbstractSerializingTestCase<RollupGroup> {

    @Override
    protected RollupGroup doParseInstance(XContentParser parser) throws IOException {
        return RollupGroup.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupGroup> instanceReader() {
        return RollupGroup::new;
    }

    @Override
    protected RollupGroup createTestInstance() {
        return randomInstance();
    }

    static RollupGroup randomInstance() {
        Map<String, RollupIndexMetadata> group = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            group.put(randomAlphaOfLength(5 + i), RollupIndexMetadataTests.randomInstance());
        }
        return new RollupGroup(group);
    }
}
