/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructureTests;

public class FindTextStructureResponseTests extends AbstractWireSerializingTestCase<FindStructureResponse> {

    @Override
    protected FindStructureResponse createTestInstance() {
        return new FindStructureResponse(TextStructureTests.createTestFileStructure());
    }

    @Override
    protected FindStructureResponse mutateInstance(FindStructureResponse response) {
        FindStructureResponse newResponse;
        do {
            newResponse = createTestInstance();
        } while (response.equals(newResponse));
        return newResponse;
    }

    @Override
    protected Writeable.Reader<FindStructureResponse> instanceReader() {
        return FindStructureResponse::new;
    }
}
