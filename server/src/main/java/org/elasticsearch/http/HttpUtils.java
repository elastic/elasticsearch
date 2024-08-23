/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

public class HttpUtils {

    static final String CLOSE = "close";
    static final String CONNECTION = "connection";
    static final String KEEP_ALIVE = "keep-alive";

    // Determine if the request connection should be closed on completion.
    public static boolean shouldCloseConnection(HttpRequest httpRequest) {
        try {
            final boolean http10 = httpRequest.protocolVersion() == HttpRequest.HttpVersion.HTTP_1_0;
            return CLOSE.equalsIgnoreCase(httpRequest.header(CONNECTION))
                || (http10 && KEEP_ALIVE.equalsIgnoreCase(httpRequest.header(CONNECTION)) == false);
        } catch (Exception e) {
            // In case we fail to parse the http protocol version out of the request we always close the connection
            return true;
        }
    }

    public static int contentLengthHeader(HttpRequest httpRequest) {
        var cl = httpRequest.getHeaders().get("content-length");
        if (cl.isEmpty()) {
            return 0;
        } else {
            return Integer.parseInt(cl.get(0));
        }
    }

    public static boolean isChunkedTransferEncoding(HttpRequest httpRequest) {
        var te = httpRequest.getHeaders().get("transfer-encoding");
        if (te.isEmpty()) {
            return false;
        } else {
            return te.get(0).equals("chunked");
        }
    }
}
