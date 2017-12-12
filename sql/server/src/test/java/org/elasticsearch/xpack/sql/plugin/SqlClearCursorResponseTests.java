/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class SqlClearCursorResponseTests extends AbstractStreamableTestCase<SqlClearCursorAction.Response> {

    @Override
    protected SqlClearCursorAction.Response createTestInstance() {
        return new SqlClearCursorAction.Response(randomBoolean());
    }

    @Override
    protected SqlClearCursorAction.Response createBlankInstance() {
        return new SqlClearCursorAction.Response();
    }

    @Override
    protected MutateFunction<SqlClearCursorAction.Response> getMutateFunction() {
        return response -> getCopyFunction().copy(response).setSucceeded(response.isSucceeded() == false);
    }
}
