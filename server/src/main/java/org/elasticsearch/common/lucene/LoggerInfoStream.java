/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
