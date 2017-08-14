/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcRequest;

public class JdbcRequestTests extends AbstractStreamableTestCase<JdbcRequest> {

    @Override
    protected JdbcRequest createTestInstance() {
        if (randomBoolean()) {
            return new JdbcRequest(new InfoRequest(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                    randomAlphaOfLength(10), randomAlphaOfLength(10)));
        } else {
            return new JdbcRequest(new MetaTableRequest(randomAlphaOfLength(10)));
        }
    }

    @Override
    protected JdbcRequest createBlankInstance() {
        return new JdbcRequest();
    }
}