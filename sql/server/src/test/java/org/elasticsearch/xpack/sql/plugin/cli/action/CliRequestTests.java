/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliRequest;

public class CliRequestTests extends AbstractStreamableTestCase<CliRequest> {

    @Override
    protected CliRequest createTestInstance() {
        if (randomBoolean()) {
            return new CliRequest(new InfoRequest(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                    randomAlphaOfLength(10), randomAlphaOfLength(10)));
        } else {
            return new CliRequest(new CommandRequest(randomAlphaOfLength(10)));
        }
    }

    @Override
    protected CliRequest createBlankInstance() {
        return new CliRequest();
    }
}