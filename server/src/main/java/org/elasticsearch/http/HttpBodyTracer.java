/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.ChunkedLoggingStream;
import org.elasticsearch.transport.NetworkTraceFlag;

import java.io.OutputStream;

class HttpBodyTracer {
    private static final Logger logger = LogManager.getLogger(HttpBodyTracer.class);

    public static boolean isEnabled() {
        return logger.isTraceEnabled();
    }

    enum Type {
        REQUEST("request"),
        RESPONSE("response");

        final String text;

        Type(String text) {
            this.text = text;
        }
    }

    static OutputStream getBodyOutputStream(long requestId, Type type) {
        try {
            if (NetworkTraceFlag.TRACE_ENABLED) {
                return ChunkedLoggingStream.create(
                    logger,
                    Level.TRACE,
                    "[" + requestId + "] " + type.text + " body",
                    ReferenceDocs.HTTP_TRACER
                );
            } else {
                logger.trace("set system property [{}] to [true] to enable HTTP body tracing", NetworkTraceFlag.PROPERTY_NAME);
            }
        } catch (Exception e) {
            assert false : e; // nothing really to go wrong here
        }
        return OutputStream.nullOutputStream();
    }
}
