/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;

public class CommandResponseTests extends ESTestCase {
    static CommandResponse randomCommandResponse() {
        long start = randomNonNegativeLong();
        long end = randomValueOtherThanMany(l -> l >= start, ESTestCase::randomNonNegativeLong);
        return new CommandResponse(start, end, randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTrip(randomCommandResponse(), (response, out) -> {
                // NOCOMMIT make this simpler
                response.encode(out);
                out.writeUTF(response.data.toString());
            }, in -> {
                // NOCOMMIT make this simpler
                CommandResponse response = (CommandResponse) ProtoUtils.readResponse(in, in.readInt());
                return new CommandResponse(response.serverTimeQueryReceived, response.serverTimeResponseSent,
                        response.requestId, in.readUTF());
            });
    }
}
