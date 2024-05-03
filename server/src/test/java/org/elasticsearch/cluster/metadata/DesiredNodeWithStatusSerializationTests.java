/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.DesiredNodeSerializationTests.mutateDesiredNode;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodeWithStatus;

public class DesiredNodeWithStatusSerializationTests extends AbstractXContentSerializingTestCase<DesiredNodeWithStatus> {

    @Override
    protected DesiredNodeWithStatus doParseInstance(XContentParser parser) throws IOException {
        return DesiredNodeWithStatus.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DesiredNodeWithStatus> instanceReader() {
        return DesiredNodeWithStatus::readFrom;
    }

    @Override
    protected DesiredNodeWithStatus createTestInstance() {
        return randomDesiredNodeWithStatus();
    }

    @Override
    protected DesiredNodeWithStatus mutateInstance(DesiredNodeWithStatus instance) {
        return mutateDesiredNodeWithStatus(instance);
    }

    public static DesiredNodeWithStatus mutateDesiredNodeWithStatus(DesiredNodeWithStatus instance) {
        if (randomBoolean()) {
            return new DesiredNodeWithStatus(
                instance.desiredNode(),
                instance.status() == DesiredNodeWithStatus.Status.PENDING
                    ? DesiredNodeWithStatus.Status.ACTUALIZED
                    : DesiredNodeWithStatus.Status.PENDING
            );
        } else {
            return new DesiredNodeWithStatus(mutateDesiredNode(instance.desiredNode()), instance.status());
        }
    }
}
