/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class DesiredNodesSerializationTests extends AbstractWireSerializingTestCase<DesiredNodes> {
    @Override
    protected Writeable.Reader<DesiredNodes> instanceReader() {
        return DesiredNodes::new;
    }

    @Override
    protected DesiredNodes createTestInstance() {
        return randomDesiredNodes();
    }

    @Override
    protected DesiredNodes mutateInstance(DesiredNodes instance) throws IOException {
        if (randomBoolean()) {
            return new DesiredNodes(instance.historyID(), instance.version() + 1, instance.nodes());
        }
        return new DesiredNodes(randomAlphaOfLength(10), instance.version(), instance.nodes());
    }

    public static DesiredNodes randomDesiredNodes() {
        List<DesiredNode> nodes = randomList(0, 10, DesiredNodeSerializationTests::randomDesiredNode);
        return new DesiredNodes(randomAlphaOfLength(10), randomIntBetween(1, 10), nodes);
    }
}
