/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructureTests;

public class FindFileStructureActionResponseTests extends AbstractWireSerializingTestCase<FindFileStructureAction.Response> {

    @Override
    protected FindFileStructureAction.Response createTestInstance() {
        return new FindFileStructureAction.Response(FileStructureTests.createTestFileStructure());
    }

    @Override
    protected Writeable.Reader<FindFileStructureAction.Response> instanceReader() {
        return FindFileStructureAction.Response::new;
    }
}
