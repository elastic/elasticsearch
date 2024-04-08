/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class UpdateDesiredNodesRequestSerializationTests extends AbstractWireSerializingTestCase<UpdateDesiredNodesRequest> {
    @Override
    protected Writeable.Reader<UpdateDesiredNodesRequest> instanceReader() {
        return UpdateDesiredNodesRequest::new;
    }

    @Override
    protected UpdateDesiredNodesRequest mutateInstance(UpdateDesiredNodesRequest request) {
        return new UpdateDesiredNodesRequest(request.getHistoryID(), request.getVersion() + 1, request.getNodes(), request.isDryRun());
    }

    @Override
    protected UpdateDesiredNodesRequest createTestInstance() {
        return DesiredNodesTestCase.randomUpdateDesiredNodesRequest();
    }
}
