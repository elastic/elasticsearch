/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetFileRolesRequestTests extends AbstractWireSerializingTestCase<GetFileRolesRequest> {

    @Override
    protected GetFileRolesRequest createTestInstance() {
        return new GetFileRolesRequest(generateRandomStringArray(randomIntBetween(1,10), randomIntBetween(5, 10), false, true));
    }

    @Override
    protected Writeable.Reader<GetFileRolesRequest> instanceReader() {
        return GetFileRolesRequest::new;
    }

    @Override
    protected GetFileRolesRequest mutateInstance(GetFileRolesRequest instance) {
        boolean givenInstanceEmpty = instance.nodesIds().length == 0;
        return new GetFileRolesRequest(randomValueOtherThan(instance.nodesIds(),
            () -> generateRandomStringArray(randomIntBetween(1,10), randomIntBetween(5, 10), false, givenInstanceEmpty == false)));
    }
}
