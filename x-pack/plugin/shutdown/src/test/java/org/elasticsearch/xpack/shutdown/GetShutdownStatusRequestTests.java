/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GetShutdownStatusRequestTests extends AbstractWireSerializingTestCase<GetShutdownStatusAction.Request> {

    @Override
    protected Writeable.Reader<GetShutdownStatusAction.Request> instanceReader() {
        return GetShutdownStatusAction.Request::readFrom;
    }

    @Override
    protected GetShutdownStatusAction.Request createTestInstance() {
        return new GetShutdownStatusAction.Request(
            randomList(0, 20, () -> randomAlphaOfLengthBetween(15, 25)).toArray(Strings.EMPTY_ARRAY)
        );
    }

    @Override
    protected GetShutdownStatusAction.Request mutateInstance(GetShutdownStatusAction.Request instance) throws IOException {
        Set<String> oldIds = new HashSet<>(Arrays.asList(instance.getNodeIds()));
        String[] newNodeIds = randomList(1, 20, () -> randomValueOtherThanMany(oldIds::contains, () -> randomAlphaOfLengthBetween(15, 25)))
            .toArray(Strings.EMPTY_ARRAY);

        return new GetShutdownStatusAction.Request(newNodeIds);
    }
}
