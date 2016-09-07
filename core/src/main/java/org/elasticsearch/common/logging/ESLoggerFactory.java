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
import org.apache.logging.log4j.message.MessageFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.Locale;
import java.util.function.Function;

/**
 * Factory to get {@link Logger}s
 */
public abstract class ESLoggerFactory {

    public static final Setting<Level> LOG_DEFAULT_LEVEL_SETTING =
        new Setting<>("logger.level", Level.INFO.name(), Level::valueOf, Property.NodeScope);
    public static final Setting<Level> LOG_LEVEL_SETTING =
        Setting.prefixKeySetting("logger.", Level.INFO.name(), Level::valueOf,
            Property.Dynamic, Property.NodeScope);

    public static Logger getLogger(String prefix, String name) {
        name = name.intern();
        final Logger logger = getLogger(new PrefixMessageFactory(), name);
        final MessageFactory factory = logger.getMessageFactory();
        // in some cases, we initialize the logger before we are ready to set the prefix
        // we can not re-initialize the logger, so the above getLogger might return an existing
        // instance without the prefix set; thus, we hack around this by resetting the prefix
        if (prefix != null && factory instanceof PrefixMessageFactory) {
            ((PrefixMessageFactory) factory).setPrefix(prefix.intern());
        }
        return logger;
    }

    public static Logger getLogger(MessageFactory messageFactory, String name) {
        return LogManager.getLogger(name, messageFactory);
    }

    public static Logger getLogger(String name) {
        return getLogger((String)null, name);
    }

    public static DeprecationLogger getDeprecationLogger(String name) {
        return new DeprecationLogger(getLogger(name));
    }

    public static DeprecationLogger getDeprecationLogger(String prefix, String name) {
        return new DeprecationLogger(getLogger(prefix, name));
    }

    public static Logger getRootLogger() {
        return LogManager.getRootLogger();
    }

    private ESLoggerFactory() {
        // Utility class can't be built.
    }

}
