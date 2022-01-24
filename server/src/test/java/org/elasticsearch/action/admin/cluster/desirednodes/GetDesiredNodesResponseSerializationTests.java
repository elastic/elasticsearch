/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GetDesiredNodesResponseSerializationTests extends AbstractWireSerializingTestCase<GetDesiredNodesAction.Response> {
    @Override
    protected Writeable.Reader<GetDesiredNodesAction.Response> instanceReader() {
        return GetDesiredNodesAction.Response::new;
    }

    @Override
    protected GetDesiredNodesAction.Response createTestInstance() {
        return new GetDesiredNodesAction.Response((DesiredNodes) null);
    }

    @Override
    protected GetDesiredNodesAction.Response mutateInstance(GetDesiredNodesAction.Response instance) throws IOException {
        return super.mutateInstance(instance);
    }
}
