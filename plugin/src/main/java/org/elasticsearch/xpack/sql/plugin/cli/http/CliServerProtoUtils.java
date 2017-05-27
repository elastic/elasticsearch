/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.http;

import java.io.DataOutputStream;
import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

public abstract class CliServerProtoUtils {

    public static BytesReference write(Response response) throws IOException {
        try (BytesStreamOutput array = new BytesStreamOutput();
             DataOutputStream out = new DataOutputStream(array)) {
            ProtoUtils.write(out, response);

            // serialize payload (if present)
            if (response instanceof CommandResponse) {
                RowSetCursor cursor = (RowSetCursor) ((CommandResponse) response).data;

                if (cursor != null) {
                    out.writeUTF(CliUtils.toString(cursor));
                }
            }

            out.flush();
            return array.bytes();
        }
    }
}