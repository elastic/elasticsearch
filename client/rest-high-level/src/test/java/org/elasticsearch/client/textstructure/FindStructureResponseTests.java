/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.textstructure;

import org.elasticsearch.client.textstructure.structurefinder.TextStructureTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class FindStructureResponseTests extends AbstractXContentTestCase<FindStructureResponse> {

    @Override
    protected FindStructureResponse createTestInstance() {
        return new FindStructureResponse(TextStructureTests.createTestFileStructure());
    }

    @Override
    protected FindStructureResponse doParseInstance(XContentParser parser) throws IOException {
        return FindStructureResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }
}
