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

package org.elasticsearch.cloud.jclouds.logging;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.jclouds.logging.BaseLogger;
import org.jclouds.logging.Logger;
import org.jclouds.logging.config.LoggingModule;

/**
 * @author kimchy (shay.banon)
 */
public class JCloudsLoggingModule extends LoggingModule {

    private final Settings settings;

    public JCloudsLoggingModule(Settings settings) {
        this.settings = settings;
    }

    @Override public Logger.LoggerFactory createLoggerFactory() {
        return new Logger.LoggerFactory() {
            @Override public Logger getLogger(String s) {
                return new JCloudsESLogger(Loggers.getLogger(s.replace("org.jclouds", "cloud.jclouds"), settings));
            }
        };
    }

    private static class JCloudsESLogger extends BaseLogger {

        private final ESLogger logger;

        private JCloudsESLogger(ESLogger logger) {
            this.logger = logger;
        }

        @Override protected void logError(String s, Throwable throwable) {
            logger.error(s, throwable);
        }

        @Override protected void logError(String s) {
            logger.error(s);
        }

        @Override protected void logWarn(String s, Throwable throwable) {
            logger.warn(s, throwable);
        }

        @Override protected void logWarn(String s) {
            logger.warn(s);
        }

        @Override protected void logInfo(String s) {
            logger.info(s);
        }

        @Override protected void logDebug(String s) {
            logger.debug(s);
        }

        @Override protected void logTrace(String s) {
            logger.trace(s);
        }

        @Override public String getCategory() {
            return logger.getName();
        }

        @Override public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }
    }
}
