/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.datastreams.ModifyDataStreamsAction.Request;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.DataStreamActionTests;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ModifyDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        final int numActions = randomIntBetween(1, 10);
        List<DataStreamAction> actions = new ArrayList<>();
        for (int k = 1; k <= numActions; k++) {
            actions.add(DataStreamActionTests.createTestInstance());
        }
        return new Request(actions);
    }

    @Override
    protected Request mutateInstance(Request request) throws IOException {
        final int moreActions = randomIntBetween(1, 5);
        List<DataStreamAction> actions = new ArrayList<>(request.getActions());
        for (int k = 1; k <= moreActions; k++) {
            actions.add(DataStreamActionTests.createTestInstance());
        }
        return new Request(actions);
    }
}
