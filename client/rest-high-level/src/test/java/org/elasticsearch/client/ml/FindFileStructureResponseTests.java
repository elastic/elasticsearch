/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.filestructurefinder.FileStructureTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class FindFileStructureResponseTests extends AbstractXContentTestCase<FindFileStructureResponse> {

    @Override
    protected FindFileStructureResponse createTestInstance() {
        return new FindFileStructureResponse(FileStructureTests.createTestFileStructure());
    }

    @Override
    protected FindFileStructureResponse doParseInstance(XContentParser parser) throws IOException {
        return FindFileStructureResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }
}
