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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * Factory to get {@link Logger}s
 */
public final class ESLoggerFactory {

    private ESLoggerFactory() {

    }

    public static final Setting<Level> LOG_DEFAULT_LEVEL_SETTING =
        new Setting<>("logger.level", Level.INFO.name(), Level::valueOf, Property.NodeScope);
    public static final Setting.AffixSetting<Level> LOG_LEVEL_SETTING =
        Setting.prefixKeySetting("logger.", (key) -> new Setting<>(key, Level.INFO.name(), Level::valueOf, Property.Dynamic,
            Property.NodeScope));

    public static Logger getLogger(String prefix, String name) {
        return getLogger(prefix, LogManager.getLogger(name));
    }

    public static Logger getLogger(String prefix, Class<?> clazz) {
        /*
         * Do not use LogManager#getLogger(Class) as this now uses Class#getCanonicalName under the hood; as this returns null for local and
         * anonymous classes, any place we create, for example, an abstract component defined as an anonymous class (e.g., in tests) will
         * result in a logger with a null name which will blow up in a lookup inside of Log4j.
         */
        return getLogger(prefix, LogManager.getLogger(clazz.getName()));
    }

    public static Logger getLogger(String prefix, Logger logger) {
        return new PrefixLogger((ExtendedLogger)logger, logger.getName(), prefix);
    }

    public static Logger getLogger(Class<?> clazz) {
        return getLogger(null, clazz);
    }

    public static Logger getLogger(String name) {
        return getLogger(null, name);
    }

    public static Logger getRootLogger() {
        return LogManager.getRootLogger();
    }

}
