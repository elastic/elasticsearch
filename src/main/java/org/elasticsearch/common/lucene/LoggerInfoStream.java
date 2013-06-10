/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.google.common.base.Charsets;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * A {@link java.io.PrintStream} that logs each {@link #println(String)} into a logger
 * under trace level.
 * <p/>
 * <p>Provides also factory methods that basically append to the logger name provide the
 * {@link #SUFFIX}.
 *
 *
 */
public class LoggerInfoStream extends PrintStream {

    public static final String SUFFIX = ".lucene";

    /**
     * Creates a new {@link LoggerInfoStream} based on the provided logger
     * by appending to its <tt>NAME</tt> the {@link #SUFFIX}.
     */
    public static LoggerInfoStream getInfoStream(ESLogger logger) {
        try {
            return new LoggerInfoStream(Loggers.getLogger(logger, SUFFIX));
        } catch (UnsupportedEncodingException e) {
            // no UTF-8 ?
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link LoggerInfoStream} based on the provided name
     * by appending to it the {@link #SUFFIX}.
     */
    public static LoggerInfoStream getInfoStream(String name) {
        try {
            return new LoggerInfoStream(Loggers.getLogger(name + SUFFIX));
        } catch (UnsupportedEncodingException e) {
            // no UTF-8 ?
            throw new RuntimeException(e);
        }
    }

    private final ESLogger logger;

    /**
     * Constucts a new instance based on the provided logger. Will output
     * each {@link #println(String)} operation as a trace level.
     * @throws UnsupportedEncodingException 
     */
    public LoggerInfoStream(ESLogger logger) throws UnsupportedEncodingException {
        super((OutputStream) null, false, Charsets.UTF_8.name());
        this.logger = logger;
    }

    /**
     * Override only the method Lucene actually uses.
     */
    @Override
    public void println(String x) {
        logger.trace(x);
    }
}
