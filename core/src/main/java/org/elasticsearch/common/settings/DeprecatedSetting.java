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
package org.elasticsearch.common.settings;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.function.Function;

/**
 * {@code DeprecatedSetting} is a proxy class for regular {@link Setting}s that acknowledges the use of deprecated settings by warning
 * users about their usage. The expected usage of this class is to mark settings that are being phased out or replaced by using the
 * deprecated variant (this) as the "fallback setting" for the replacement.
 * <p>
 * Instances should be constructed using {@link Setting#deprecatedSetting}.
 */
class DeprecatedSetting<T> extends Setting<T> {
    /**
     * Enables warning on deprecated setting usage.
     */
    private static final ESLogger logger = Loggers.getLogger(DeprecatedSetting.class);

    /**
     * The setting that is wrapped and deprecated.
     */
    private final Setting<T> setting;
    /**
     * The key replacing the current one, if any.
     */
    private final String replacementKey;

    /**
     * Creates a new {@link DeprecatedSetting} instance.
     *
     * @param setting the setting being deprecated (whose values <em>must</em> match)
     * @param replacementKey the key replacing this one, if any
     * @param key the settings key for this setting.
     * @param defaultValue a default value function that returns the default values string representation.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param dynamic true iff this setting can be dynamically updateable
     * @param scope the scope of this setting
     */
    public DeprecatedSetting(Setting<T> setting, String replacementKey,
                             String key,
                             Function<Settings, String> defaultValue, Function<String, T> parser,
                             boolean dynamic, Scope scope) {
        super(key, defaultValue, parser, dynamic, scope);

        assert setting != null;

        this.setting = setting;
        this.replacementKey = replacementKey;
    }

    /**
     * {@inheritDoc}
     * <p>
     * In addition to the default behavior, this will also warn users if the setting is actually supplied (and therefore used in
     * any way).
     */
    @Override
    public String getRaw(Settings settings) {
        // They're using the setting, so we need to tell them to stop
        if (setting.exists(settings)) {
            // if we were given a key, then we can advertise it
            if (replacementKey != null) {
                logger.warn("[{}] setting is deprecated and it will be removed in a future release! Use the updated setting [{}]",
                            getKey(), replacementKey);
            }
            else {
                // It would be convenient to show its replacement key, but replacement is often not so simple
                logger.warn("[{}] setting is deprecated and it will be removed in a future release! " +
                            "See the breaking changes lists in the documentation for details",
                            getKey());
            }
        }

        return setting.getRaw(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean isGroupSetting() {
        return setting.isGroupSetting();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean hasComplexMatcher() {
        return setting.hasComplexMatcher();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(Settings settings) {
        return setting.get(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean match(String toTest) {
        return setting.match(toTest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Setting<T> getConcreteSetting(String key) {
        return setting.getConcreteSetting(key);
    }
}
