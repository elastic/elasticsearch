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

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Locale;

/**
 * Factory to get {@link ESLogger}s
 */
public abstract class ESLoggerFactory {

    public static final Setting<LogLevel> LOG_DEFAULT_LEVEL_SETTING =
        new Setting<>("logger.level", LogLevel.INFO.name(), LogLevel::parse, Property.NodeScope);
    public static final Setting<LogLevel> LOG_LEVEL_SETTING =
        Setting.prefixKeySetting("logger.", LogLevel.INFO.name(), LogLevel::parse,
            Property.Dynamic, Property.NodeScope);

    public static ESLogger getLogger(String prefix, String name) {
        prefix = prefix == null ? null : prefix.intern();
        name = name.intern();
        return new ESLogger(prefix, Logger.getLogger(name));
    }

    public static ESLogger getLogger(String name) {
        return getLogger(null, name);
    }

    public static DeprecationLogger getDeprecationLogger(String name) {
        return new DeprecationLogger(getLogger(name));
    }

    public static DeprecationLogger getDeprecationLogger(String prefix, String name) {
        return new DeprecationLogger(getLogger(prefix, name));
    }

    public static ESLogger getRootLogger() {
        return new ESLogger(null, Logger.getRootLogger());
    }

    private ESLoggerFactory() {
        // Utility class can't be built.
    }

    public enum LogLevel {
        WARN, TRACE, INFO, DEBUG, ERROR;
        public static LogLevel parse(String level) {
            return valueOf(level.toUpperCase(Locale.ROOT));
        }
    }
}
