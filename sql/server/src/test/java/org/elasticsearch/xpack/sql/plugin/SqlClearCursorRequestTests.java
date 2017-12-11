/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.sql.session.Cursor;

import static org.elasticsearch.xpack.sql.execution.search.ScrollCursorTests.randomScrollCursor;

public class SqlClearCursorRequestTests extends AbstractStreamableTestCase<SqlClearCursorAction.Request> {

    @Override
    protected SqlClearCursorAction.Request createTestInstance() {
        return new SqlClearCursorAction.Request(randomScrollCursor());
    }

    @Override
    protected SqlClearCursorAction.Request createBlankInstance() {
        return new SqlClearCursorAction.Request();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MutateFunction<SqlClearCursorAction.Request> getMutateFunction() {
        return request -> getCopyFunction().copy(request).setCursor(randomScrollCursor());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Cursor.getNamedWriteables());
    }

}
