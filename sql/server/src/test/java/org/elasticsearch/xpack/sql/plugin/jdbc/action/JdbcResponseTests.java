/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcResponse;

import java.util.Collections;

public class JdbcResponseTests extends AbstractStreamableTestCase<JdbcResponse> {

    @Override
    protected JdbcResponse createTestInstance() {
        if (randomBoolean()) {
            return new JdbcResponse(new InfoResponse(randomAlphaOfLength(10), randomAlphaOfLength(10),
                    randomByte(), randomByte(),
                    randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10)));
        } else {
            return new JdbcResponse(new MetaTableResponse(Collections.singletonList(randomAlphaOfLength(10))));
        }
    }

    @Override
    protected JdbcResponse createBlankInstance() {
        return new JdbcResponse();
    }
}