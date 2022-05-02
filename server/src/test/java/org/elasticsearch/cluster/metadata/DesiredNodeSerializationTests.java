/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DesiredNodeSerializationTests extends AbstractSerializingTestCase<DesiredNode> {
    @Override
    protected DesiredNode doParseInstance(XContentParser parser) throws IOException {
        return DesiredNode.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DesiredNode> instanceReader() {
        return DesiredNode::new;
    }

    @Override
    protected DesiredNode createTestInstance() {
        return DesiredNodesTestCase.randomDesiredNodeWithRandomSettings();
    }
}
