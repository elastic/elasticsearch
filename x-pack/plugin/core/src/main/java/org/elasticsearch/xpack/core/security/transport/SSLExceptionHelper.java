/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport;


import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.NotSslRecordException;

import javax.net.ssl.SSLException;

public class SSLExceptionHelper {

    private SSLExceptionHelper() {
    }

    public static boolean isNotSslRecordException(Throwable e) {
        return e instanceof DecoderException &&
                e.getCause() instanceof NotSslRecordException;
    }

    public static boolean isCloseDuringHandshakeException(Throwable e) {
        return e instanceof SSLException
                && e.getCause() == null
                && "Received close_notify during handshake".equals(e.getMessage());
    }

    public static boolean isReceivedCertificateUnknownException(Throwable e) {
        return e instanceof DecoderException
                && e.getCause() instanceof SSLException
                && "Received fatal alert: certificate_unknown".equals(e.getCause().getMessage());
    }
}
