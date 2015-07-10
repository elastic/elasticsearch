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

package org.elasticsearch.common.logging;

import org.elasticsearch.common.logging.jdk.JdkESLoggerFactory;
import org.elasticsearch.common.logging.log4j.Log4jESLoggerFactory;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;

/**
 * Factory to get {@link ESLogger}s
 */
public abstract class ESLoggerFactory {

    public static final String LOGGER_IMPL_PROPERTY_NAME = "es.logger.impl";
    private static volatile ESLoggerFactory defaultFactory = new JdkESLoggerFactory();

    static {
        defaultFactory = getConfiguredEsLoggerFactory();
    }

    static ESLoggerFactory getConfiguredEsLoggerFactory() {
        String loggingType = System.getProperty(LOGGER_IMPL_PROPERTY_NAME, "log4j");
        switch (loggingType) {
            case "slf4j":
                return new Slf4jESLoggerFactory();
            case "jdk":
                return new JdkESLoggerFactory();
            case "log4j":
                return new Log4jESLoggerFactory();
            default:
                throw new IllegalArgumentException("Unknown logger impl configured: " + LOGGER_IMPL_PROPERTY_NAME + "[" + loggingType + "]");
        }
    }

    /**
     * Changes the default factory.
     */
    public static void setDefaultFactory(ESLoggerFactory defaultFactory) {
        if (defaultFactory == null) {
            throw new NullPointerException("defaultFactory");
        }
        ESLoggerFactory.defaultFactory = defaultFactory;
    }


    public static ESLogger getLogger(String prefix, String name) {
        return defaultFactory.newInstance(prefix == null ? null : prefix.intern(), name.intern());
    }

    public static ESLogger getLogger(String name) {
        return defaultFactory.newInstance(name.intern());
    }

    public static DeprecationLogger getDeprecationLogger(String name) {
        return new DeprecationLogger(getLogger(name));
    }

    public static DeprecationLogger getDeprecationLogger(String prefix, String name) {
        return new DeprecationLogger(getLogger(prefix, name));
    }

    public static ESLogger getRootLogger() {
        return defaultFactory.rootLogger();
    }

    public ESLogger newInstance(String name) {
        return newInstance(null, name);
    }

    protected abstract ESLogger rootLogger();

    protected abstract ESLogger newInstance(String prefix, String name);
}
