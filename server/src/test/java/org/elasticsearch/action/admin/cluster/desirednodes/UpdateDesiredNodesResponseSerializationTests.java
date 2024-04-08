/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class UpdateDesiredNodesResponseSerializationTests extends AbstractWireSerializingTestCase<UpdateDesiredNodesResponse> {
    @Override
    protected Writeable.Reader<UpdateDesiredNodesResponse> instanceReader() {
        return UpdateDesiredNodesResponse::new;
    }

    @Override
    protected UpdateDesiredNodesResponse createTestInstance() {
        return new UpdateDesiredNodesResponse(randomBoolean());
    }

    @Override
    protected UpdateDesiredNodesResponse mutateInstance(UpdateDesiredNodesResponse instance) {
        return new UpdateDesiredNodesResponse(instance.hasReplacedExistingHistoryId() == false);
    }
}
