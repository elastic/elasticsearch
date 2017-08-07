/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliResponse;

public class CliResponseTests extends AbstractStreamableTestCase<CliResponse> {

    @Override
    protected CliResponse createTestInstance() {
        if (randomBoolean()) {
            return new CliResponse(new InfoResponse(randomAlphaOfLength(10), randomAlphaOfLength(10),
                    randomByte(), randomByte(),
                    randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10)));
        } else {
            return new CliResponse(new CommandResponse(randomNonNegativeLong(), randomNonNegativeLong(),
                    randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
    }

    @Override
    protected CliResponse createBlankInstance() {
        return new CliResponse();
    }
}