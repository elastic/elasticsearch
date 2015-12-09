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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 */
public class Setting<T> extends ToXContentToBytes {
    private final String key;
    private final Function<Settings, String> defaultValue;
    private final Function<String, T> parser;
    private final boolean dynamic;
    private final Scope scope;

    public Setting(String key, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.parser = parser;
        this.dynamic = dynamic;
        this.scope = scope;
    }

    /**
     * Returns the settings key or a prefix if this setting is a group setting
     * @see #isGroupSetting()
     */
    public final String getKey() {
        return key;
    }

    /**
     * Returns <code>true</code> iff this setting is dynamically updateable, otherwise <code>false</code>
     */
    public final boolean isDynamic() {
        return dynamic;
    }

    /**
     * Returns the settings scope
     */
    public final Scope getScope() {
        return scope;
    }

    /**
     * Returns <code>true</code> iff this setting is a group setting. Group settings represent a set of settings
     * rather than a single value. The key, see {@link #getKey()}, in contrast to non-group settings is a prefix like <tt>cluster.store.</tt>
     * that matches all settings with this prefix.
     */
    public boolean isGroupSetting() {
        return false;
    }

    /**
     * Returns the default values string representation for this setting.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    public final String getDefault(Settings settings) {
        return defaultValue.apply(settings);
    }

    /**
     * Returns <code>true</code> iff this setting is present in the given settings object. Otherwise <code>false</code>
     */
    public final boolean exists(Settings settings) {
        return settings.get(key) != null;
    }

    /**
     * Returns the settings value. If the setting is not present in the given settings object the default value is returned
     * instead.
     */
    public T get(Settings settings) {
        String value = getRaw(settings);
        try {
            return parser.apply(value);
        } catch (ElasticsearchParseException ex) {
            throw ex;
        } catch (Exception t) {
            throw new IllegalArgumentException("Failed to parse value [" + value + "] for setting [" + getKey() + "]", t);
        }
    }

    /**
     * Returns the raw (string) settings value. If the setting is not present in the given settings object the default value is returned
     * instead. This is useful if the value can't be parsed due to an invalid value to access the actual value.
     */
    public final String getRaw(Settings settings) {
        return settings.get(key, defaultValue.apply(settings));
    }

    /**
     * Returns <code>true</code> iff the given key matches the settings key or if this setting is a group setting if the
     * given key is part of the settings group.
     * @see #isGroupSetting()
     */
    public boolean match(String toTest) {
        return Regex.simpleMatch(key, toTest);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("key", key);
        builder.field("type", scope.name());
        builder.field("dynamic", dynamic);
        builder.field("default", defaultValue.apply(Settings.EMPTY));
        builder.endObject();
        return builder;
    }

    /**
     * The settings scope - settings can either be cluster settings or per index settings.
     */
    public enum Scope {
        CLUSTER,
        INDEX;
    }

    final AbstractScopedSettings.SettingUpdater newUpdater(Consumer<T> consumer, ESLogger logger, Settings settings) {
        return newUpdater(consumer, logger, settings, (s) -> {});
    }

    AbstractScopedSettings.SettingUpdater newUpdater(Consumer<T> consumer, ESLogger logger, Settings settings, Consumer<T> accept) {
        if (isDynamic()) {
            return new Updater(consumer, logger, settings, accept);
        } else {
            throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
        }
    }

    /**
     * this is used for settings that depend on each other... see {@link org.elasticsearch.common.settings.AbstractScopedSettings#addSettingsUpdateConsumer(Setting, Setting, BiConsumer)} and it's
     * usage for details.
     */
    static <A, B> AbstractScopedSettings.SettingUpdater compoundUpdater(final BiConsumer<A,B> consumer, final Setting<A> aSettting, final Setting<B> bSetting, ESLogger logger, Settings settings) {
        final AtomicReference<A> aRef = new AtomicReference<>();
        final AtomicReference<B> bRef = new AtomicReference<>();
        final AbstractScopedSettings.SettingUpdater aSettingUpdater = aSettting.newUpdater(aRef::set, logger, settings);
        final AbstractScopedSettings.SettingUpdater bSettingUpdater = bSetting.newUpdater(bRef::set, logger, settings);
        return new AbstractScopedSettings.SettingUpdater() {
            boolean aHasChanged = false;
            boolean bHasChanged = false;
            @Override
            public boolean prepareApply(Settings settings) {
                aHasChanged = aSettingUpdater.prepareApply(settings);
                bHasChanged = bSettingUpdater.prepareApply(settings);
                return aHasChanged || bHasChanged;
            }

            @Override
            public void apply() {
                aSettingUpdater.apply();
                bSettingUpdater.apply();
                if (aHasChanged || bHasChanged) {
                    consumer.accept(aRef.get(), bRef.get());
                }
            }

            @Override
            public void rollback() {
                try {
                    aRef.set(null);
                    aSettingUpdater.rollback();
                } finally {
                    bRef.set(null);
                    bSettingUpdater.rollback();
                }
            }

            @Override
            public String toString() {
                return "CompoundUpdater for: " + aSettingUpdater + " and " + bSettingUpdater;
            }
        };
    }


    private class Updater implements AbstractScopedSettings.SettingUpdater {
        private final Consumer<T> consumer;
        private final ESLogger logger;
        private final Consumer<T> accept;
        private String value;
        private boolean commitPending;
        private String pendingValue;
        private T valueInstance;

        public Updater(Consumer<T> consumer, ESLogger logger, Settings settings,  Consumer<T> accept) {
            this.consumer = consumer;
            this.logger = logger;
            value = getRaw(settings);
            this.accept = accept;
        }


        public boolean prepareApply(Settings settings) {
            final String newValue = getRaw(settings);
            if (value.equals(newValue) == false) {
                T inst = get(settings);
                try {
                    accept.accept(inst);
                } catch (Exception | AssertionError e) {
                    throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + value + "] to [" + getRaw(settings) + "]", e);
                }
                pendingValue = newValue;
                valueInstance = inst;
                commitPending = true;

            } else {
                commitPending = false;
            }
            return commitPending;
        }

        public void apply() {
            if (commitPending) {
                logger.info("update [{}] from [{}] to [{}]", key, value, pendingValue);
                value = pendingValue;
                consumer.accept(valueInstance);
            }
            commitPending = false;
            valueInstance = null;
            pendingValue = null;
        }

        public void rollback() {
            commitPending = false;
            valueInstance = null;
            pendingValue = null;
        }

        @Override
        public String toString() {
            return "Updater for: " + Setting.this.toString();
        }
    }


    public Setting(String key, String defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        this(key, (s) -> defaultValue, parser, dynamic, scope);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), Float::parseFloat, dynamic, scope);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, float minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), (s) -> {
            float value = Float.parseFloat(s);
            if (value < minValue) {
                throw new ElasticsearchParseException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return value;
        }, dynamic, scope);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), Integer::parseInt, dynamic, scope);
    }

    public static Setting<Boolean> boolSetting(String key, boolean defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Boolean.toString(defaultValue), Booleans::parseBooleanExact, dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, String percentage, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> percentage, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue value, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> value.toString(), (s) -> ByteSizeValue.parseBytesSizeValue(s, key), dynamic, scope);
    }

    public static Setting<TimeValue> positiveTimeSetting(String key, TimeValue defaultValue, boolean dynamic, Scope scope) {
        return timeSetting(key, defaultValue, TimeValue.timeValueMillis(0), dynamic, scope);
    }

    public static Setting<Settings> groupSetting(String key, boolean dynamic, Scope scope) {
        if (key.endsWith(".") == false) {
            throw new IllegalArgumentException("key must end with a '.'");
        }
        return new Setting<Settings>(key, "", (s) -> null, dynamic, scope) {

            @Override
            public boolean isGroupSetting() {
                return true;
            }

            @Override
            public Settings get(Settings settings) {
                return settings.getByPrefix(key);
            }

            @Override
            public boolean match(String toTest) {
                return Regex.simpleMatch(key + "*", toTest);
            }

            @Override
            public AbstractScopedSettings.SettingUpdater newUpdater(Consumer<Settings> consumer, ESLogger logger, Settings settings, Consumer<Settings> accept) {
                if (isDynamic() == false) {
                    throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
                }
                final Setting<?> setting = this;
                return new AbstractScopedSettings.SettingUpdater() {
                    private Settings pendingSettings;
                    private Settings committedSettings = get(settings);

                    @Override
                    public boolean prepareApply(Settings settings) {
                        Settings currentSettings = get(settings);
                        if (currentSettings.equals(committedSettings) == false) {
                            try {
                                accept.accept(currentSettings);
                            } catch (Exception | AssertionError e) {
                                throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + committedSettings.getAsMap() + "] to [" + currentSettings.getAsMap() + "]", e);
                            }
                            pendingSettings = currentSettings;
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public void apply() {
                        if (pendingSettings != null) {
                            consumer.accept(pendingSettings);
                            committedSettings = pendingSettings;
                        }
                        pendingSettings = null;
                    }

                    @Override
                    public void rollback() {
                        pendingSettings = null;
                    }

                    @Override
                    public String toString() {
                        return "Updater for: " + setting.toString();
                    }
                };
            }
        };
    }

    public static Setting<TimeValue> timeSetting(String key, Function<Settings, String> defaultValue, TimeValue minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, defaultValue, (s) -> {
            TimeValue timeValue = TimeValue.parseTimeValue(s, null, key);
            if (timeValue.millis() < minValue.millis()) {
                throw new ElasticsearchParseException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return timeValue;
        }, dynamic, scope);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, TimeValue minValue, boolean dynamic, Scope scope) {
        return timeSetting(key, (s) -> defaultValue.getStringRep(), minValue, dynamic, scope);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> defaultValue.toString(), (s) -> TimeValue.parseTimeValue(s, defaultValue, key), dynamic, scope);
    }

    public static Setting<Double> doubleSetting(String key, double defaultValue, double minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Double.toString(defaultValue), (s) -> {
            final double d = Double.parseDouble(s);
            if (d < minValue) {
                throw new ElasticsearchParseException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return d;
        }, dynamic, scope);
    }

}
