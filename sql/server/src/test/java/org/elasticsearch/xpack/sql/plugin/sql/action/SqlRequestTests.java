/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;

public class SqlRequestTests extends AbstractStreamableTestCase<SqlRequest> {

    @Override
    protected SqlRequest createTestInstance() {
        return new SqlRequest(randomAlphaOfLength(10), randomDateTimeZone(), randomBoolean() ? randomAlphaOfLength(10) : null);
    }

    @Override
    protected SqlRequest createBlankInstance() {
        return new SqlRequest();
    }
}