/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.discovery.jgroups;

import org.jgroups.logging.CustomLogFactory;
import org.jgroups.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kimchy (Shay Banon)
 */
public class JgroupsCustomLogFactory implements CustomLogFactory {

    @Override public Log getLog(Class clazz) {
        return getLog(clazz.getName());
    }

    @Override public Log getLog(String category) {
        return new Slf4jLog(LoggerFactory.getLogger(category.replace("org.jgroups.", "jgroups.").replace(".protocols.", ".")));
    }

    private static class Slf4jLog implements Log {

        private final Logger logger;

        private Slf4jLog(Logger logger) {
            this.logger = logger;
        }

        @Override public boolean isFatalEnabled() {
            return logger.isErrorEnabled();
        }

        @Override public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @Override public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override public void debug(String msg) {
            logger.debug(msg);
        }

        @Override public void debug(String msg, Throwable throwable) {
            logger.debug(msg, throwable);
        }

        @Override public void error(String msg) {
            logger.error(msg);
        }

        @Override public void error(String msg, Throwable throwable) {
            logger.error(msg, throwable);
        }

        @Override public void fatal(String msg) {
            logger.error(msg);
        }

        @Override public void fatal(String msg, Throwable throwable) {
            logger.error(msg, throwable);
        }

        @Override public void info(String msg) {
            logger.info(msg);
        }

        @Override public void info(String msg, Throwable throwable) {
            logger.info(msg, throwable);
        }

        @Override public void trace(Object msg) {
            logger.trace(msg.toString());
        }

        @Override public void trace(Object msg, Throwable throwable) {
            logger.trace(msg.toString(), throwable);
        }

        @Override public void trace(String msg) {
            logger.trace(msg);
        }

        @Override public void trace(String msg, Throwable throwable) {
            logger.trace(msg, throwable);
        }

        @Override public void warn(String msg) {
            logger.warn(msg);
        }

        @Override public void warn(String msg, Throwable throwable) {
            logger.warn(msg, throwable);
        }

        @Override public void setLevel(String level) {
            // ignore
        }

        @Override public String getLevel() {
            return null;
        }
    }
}
