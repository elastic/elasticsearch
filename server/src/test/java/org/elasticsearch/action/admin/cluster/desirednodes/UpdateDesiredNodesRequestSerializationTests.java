/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodeWithRandomSettings;

public class UpdateDesiredNodesRequestSerializationTests extends AbstractWireSerializingTestCase<UpdateDesiredNodesRequest> {
    @Override
    protected Writeable.Reader<UpdateDesiredNodesRequest> instanceReader() {
        return UpdateDesiredNodesRequest::new;
    }

    @Override
    protected UpdateDesiredNodesRequest mutateInstance(UpdateDesiredNodesRequest request) throws IOException {
        return new UpdateDesiredNodesRequest(request.getHistoryID(), request.getVersion() + 1, request.getNodes());
    }

    @Override
    protected UpdateDesiredNodesRequest createTestInstance() {
        return randomUpdateDesiredNodesRequest();
    }

    public static UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest() {
        return randomUpdateDesiredNodesRequest(Version.CURRENT);
    }

    public static UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest(Version nodesVersion) {
        return new UpdateDesiredNodesRequest(
            UUIDs.randomBase64UUID(),
            randomLongBetween(0, Long.MAX_VALUE - 1000),
            randomList(0, 100, () -> randomDesiredNodeWithRandomSettings(nodesVersion))
        );
    }
}
