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

public class FindTextStructureActionResponseTests extends AbstractWireSerializingTestCase<FindStructureAction.Response> {

    @Override
    protected FindStructureAction.Response createTestInstance() {
        return new FindStructureAction.Response(TextStructureTests.createTestFileStructure());
    }

    @Override
    protected FindStructureAction.Response mutateInstance(FindStructureAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<FindStructureAction.Response> instanceReader() {
        return FindStructureAction.Response::new;
    }
}
