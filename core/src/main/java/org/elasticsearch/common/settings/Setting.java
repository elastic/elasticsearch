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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 */
public class Setting<T> extends ToXContentToBytes {
    private final String key;
    protected final Function<Settings, String> defaultValue;
    private final Function<String, T> parser;
    private final boolean dynamic;
    private final Scope scope;

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param defaultValue a default value function that returns the default values string representation.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param dynamic true iff this setting can be dynamically updateable
     * @param scope the scope of this setting
     */
    public Setting(String key, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        assert parser.apply(defaultValue.apply(Settings.EMPTY)) != null || this.isGroupSetting(): "parser returned null";
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
    boolean isGroupSetting() {
        return false;
    }

    boolean hasComplexMatcher() {
        return isGroupSetting();
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
            throw new IllegalArgumentException(ex.getMessage(), ex);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Failed to parse value [" + value + "] for setting [" + getKey() + "]", ex);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception t) {
            throw new IllegalArgumentException("Failed to parse value [" + value + "] for setting [" + getKey() + "]", t);
        }
    }

    /**
     * Returns the raw (string) settings value. If the setting is not present in the given settings object the default value is returned
     * instead. This is useful if the value can't be parsed due to an invalid value to access the actual value.
     */
    public String getRaw(Settings settings) {
        return settings.get(key, defaultValue.apply(settings));
    }

    /**
     * Returns <code>true</code> iff the given key matches the settings key or if this setting is a group setting if the
     * given key is part of the settings group.
     * @see #isGroupSetting()
     */
    public boolean match(String toTest) {
        return key.equals(toTest);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("key", key);
        builder.field("type", scope.name());
        builder.field("dynamic", dynamic);
        builder.field("is_group_setting", isGroupSetting());
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

    final AbstractScopedSettings.SettingUpdater newUpdater(Consumer<T> consumer, ESLogger logger) {
        return newUpdater(consumer, logger, (s) -> {});
    }

    AbstractScopedSettings.SettingUpdater newUpdater(Consumer<T> consumer, ESLogger logger, Consumer<T> validator) {
        if (isDynamic()) {
            return new Updater(consumer, logger, validator);
        } else {
            throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
        }
    }

    /**
     * this is used for settings that depend on each other... see {@link org.elasticsearch.common.settings.AbstractScopedSettings#addSettingsUpdateConsumer(Setting, Setting, BiConsumer)} and it's
     * usage for details.
     */
    static <A, B> AbstractScopedSettings.SettingUpdater<Tuple<A, B>> compoundUpdater(final BiConsumer<A,B> consumer, final Setting<A> aSettting, final Setting<B> bSetting, ESLogger logger) {
        final AbstractScopedSettings.SettingUpdater<A> aSettingUpdater = aSettting.newUpdater(null, logger);
        final AbstractScopedSettings.SettingUpdater<B> bSettingUpdater = bSetting.newUpdater(null, logger);
        return new AbstractScopedSettings.SettingUpdater<Tuple<A, B>>() {
            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                return aSettingUpdater.hasChanged(current, previous) || bSettingUpdater.hasChanged(current, previous);
            }

            @Override
            public Tuple<A, B> getValue(Settings current, Settings previous) {
                return new Tuple<>(aSettingUpdater.getValue(current, previous), bSettingUpdater.getValue(current, previous));
            }

            @Override
            public void apply(Tuple<A, B> value, Settings current, Settings previous) {
                consumer.accept(value.v1(), value.v2());
            }

            @Override
            public String toString() {
                return "CompoundUpdater for: " + aSettingUpdater + " and " + bSettingUpdater;
            }
        };
    }


    private class Updater implements AbstractScopedSettings.SettingUpdater<T> {
        private final Consumer<T> consumer;
        private final ESLogger logger;
        private final Consumer<T> accept;

        public Updater(Consumer<T> consumer, ESLogger logger, Consumer<T> accept) {
            this.consumer = consumer;
            this.logger = logger;
            this.accept = accept;
        }

        @Override
        public String toString() {
            return "Updater for: " + Setting.this.toString();
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            final String newValue = getRaw(current);
            final String value = getRaw(previous);
            assert isGroupSetting() == false : "group settings must override this method";
            assert value != null : "value was null but can't be unless default is null which is invalid";

            return value.equals(newValue) == false;
        }

        @Override
        public T getValue(Settings current, Settings previous) {
            final String newValue = getRaw(current);
            final String value = getRaw(previous);
            T inst = get(current);
            try {
                accept.accept(inst);
            } catch (Exception | AssertionError e) {
                throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + value + "] to [" + newValue + "]", e);
            }
            return inst;
        }

        @Override
        public void apply(T value, Settings current, Settings previous) {
            logger.info("update [{}] from [{}] to [{}]", key, getRaw(previous), getRaw(current));
            consumer.accept(value);
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
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return value;
        }, dynamic, scope);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, key), dynamic, scope);
    }

    public static int parseInt(String s, int minValue, String key) {
        int value = Integer.parseInt(s);
        if (value < minValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        return value;
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, boolean dynamic, Scope scope) {
        return intSetting(key, defaultValue, Integer.MIN_VALUE, dynamic, scope);
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

    public static <T> Setting<List<T>> listSetting(String key, List<String> defaultStringValue, Function<String, T> singleValueParser, boolean dynamic, Scope scope) {
        return listSetting(key, (s) -> defaultStringValue, singleValueParser, dynamic, scope);
    }
    public static <T> Setting<List<T>> listSetting(String key, Function<Settings, List<String>> defaultStringValue, Function<String, T> singleValueParser, boolean dynamic, Scope scope) {
        Function<String, List<T>> parser = (s) -> {
            try (XContentParser xContentParser = XContentType.JSON.xContent().createParser(s)){
                XContentParser.Token token = xContentParser.nextToken();
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("expected START_ARRAY but got " + token);
                }
                ArrayList<T> list = new ArrayList<>();
                while ((token = xContentParser.nextToken()) !=XContentParser.Token.END_ARRAY) {
                    if (token != XContentParser.Token.VALUE_STRING) {
                        throw new IllegalArgumentException("expected VALUE_STRING but got " + token);
                    }
                    list.add(singleValueParser.apply(xContentParser.text()));
                }
                return list;
            } catch (IOException e) {
                throw new IllegalArgumentException("failed to parse array", e);
            }
        };
        return new Setting<List<T>>(key, (s) -> arrayToParsableString(defaultStringValue.apply(s).toArray(Strings.EMPTY_ARRAY)), parser, dynamic, scope) {
            private final Pattern pattern = Pattern.compile(Pattern.quote(key)+"(\\.\\d+)?");
            @Override
            public String getRaw(Settings settings) {
                String[] array = settings.getAsArray(key, null);
                return array == null ? defaultValue.apply(settings) : arrayToParsableString(array);
            }

            public boolean match(String toTest) {
                return pattern.matcher(toTest).matches();
            }

            @Override
            boolean hasComplexMatcher() {
                return true;
            }
        };
    }

    private static String arrayToParsableString(String[] array) {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startArray();
            for (String element : array) {
                builder.value(element);
            }
            builder.endArray();
            return builder.string();
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
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
            public AbstractScopedSettings.SettingUpdater<Settings> newUpdater(Consumer<Settings> consumer, ESLogger logger, Consumer<Settings> validator) {
                if (isDynamic() == false) {
                    throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
                }
                final Setting<?> setting = this;
                return new AbstractScopedSettings.SettingUpdater<Settings>() {

                    @Override
                    public boolean hasChanged(Settings current, Settings previous) {
                        Settings currentSettings = get(current);
                        Settings previousSettings = get(previous);
                        return currentSettings.equals(previousSettings) == false;
                    }

                    @Override
                    public Settings getValue(Settings current, Settings previous) {
                        Settings currentSettings = get(current);
                        Settings previousSettings = get(previous);
                        try {
                            validator.accept(currentSettings);
                        } catch (Exception | AssertionError e) {
                            throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + previousSettings.getAsMap() + "] to [" + currentSettings.getAsMap() + "]", e);
                        }
                        return currentSettings;
                    }

                    @Override
                    public void apply(Settings value, Settings current, Settings previous) {
                        consumer.accept(value);
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
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
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
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return d;
        }, dynamic, scope);
    }

}
