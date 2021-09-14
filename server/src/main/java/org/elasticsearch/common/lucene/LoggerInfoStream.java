/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.common.logging.Loggers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** An InfoStream (for Lucene's IndexWriter) that redirects
 *  messages to "lucene.iw.ifd" and "lucene.iw" Logger.trace. */
public final class LoggerInfoStream extends InfoStream {

    private final Logger parentLogger;

    private final Map<String, Logger> loggers = new ConcurrentHashMap<>();

    public LoggerInfoStream(final Logger parentLogger) {
        this.parentLogger = parentLogger;
    }

    @Override
    public void message(String component, String message) {
        getLogger(component).trace("{} {}: {}", Thread.currentThread().getName(), component, message);
    }

    @Override
    public boolean isEnabled(String component) {
        // TP is a special "test point" component; we don't want
        // to log it:
        return getLogger(component).isTraceEnabled() && component.equals("TP") == false;
    }

    private Logger getLogger(String component) {
        return loggers.computeIfAbsent(component, c -> Loggers.getLogger(parentLogger, "." + c));
    }

    @Override
    public void close() {

    }

}
