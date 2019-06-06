/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructureTests;

public class FindFileStructureActionResponseTests extends AbstractStreamableTestCase<FindFileStructureAction.Response> {

    @Override
    protected FindFileStructureAction.Response createTestInstance() {
        return new FindFileStructureAction.Response(FileStructureTests.createTestFileStructure());
    }

    @Override
    protected FindFileStructureAction.Response createBlankInstance() {
        return new FindFileStructureAction.Response();
    }
}
