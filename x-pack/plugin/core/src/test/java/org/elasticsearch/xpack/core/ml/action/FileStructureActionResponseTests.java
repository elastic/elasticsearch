/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructureTests;

public class FileStructureActionResponseTests extends AbstractStreamableTestCase<FileStructureAction.Response> {

    @Override
    protected FileStructureAction.Response createTestInstance() {
        return new FileStructureAction.Response(FileStructureTests.createTestFileStructure());
    }

    @Override
    protected FileStructureAction.Response createBlankInstance() {
        return new FileStructureAction.Response();
    }
}
