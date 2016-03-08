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
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A setting. Encapsulates typical stuff like default value, parsing, and scope.
 * Some (dynamic=true) can by modified at run time using the API.
 * All settings inside elasticsearch or in any of the plugins should use this type-safe and generic settings infrastructure
 * together with {@link AbstractScopedSettings}. This class contains several utility methods that makes it straight forward
 * to add settings for the majority of the cases. For instance a simple boolean settings can be defined like this:
 * <pre>{@code
 * public static final Setting<Boolean>; MY_BOOLEAN = Setting.boolSetting("my.bool.setting", true, false, Scope.CLUSTER);}
 * </pre>
 * To retrieve the value of the setting a {@link Settings} object can be passed directly to the {@link Setting#get(Settings)} method.
 * <pre>
 * final boolean myBooleanValue = MY_BOOLEAN.get(settings);
 * </pre>
 * It's recommended to use typed settings rather than string based settings. For example adding a setting for an enum type:
 * <pre>{@code
 * public enum Color {
 *     RED, GREEN, BLUE;
 * }
 * public static final Setting<Color> MY_BOOLEAN = new Setting<>("my.color.setting", Color.RED.toString(), Color::valueOf, false, Scope.CLUSTER);
 * }
 * </pre>
 */
public class Setting<T> extends ToXContentToBytes {
    private final Key key;
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
    public Setting(Key key, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        assert parser.apply(defaultValue.apply(Settings.EMPTY)) != null || this.isGroupSetting(): "parser returned null";
        this.key = key;
        this.defaultValue = defaultValue;
        this.parser = parser;
        this.dynamic = dynamic;
        this.scope = scope;
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param defaultValue a default value function that returns the default values string representation.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param dynamic true iff this setting can be dynamically updateable
     * @param scope the scope of this setting
     */
    public Setting(String key, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        this(new SimpleKey(key), defaultValue, parser, dynamic, scope);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param fallBackSetting a setting to fall back to if the current setting is not set.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param dynamic true iff this setting can be dynamically updateable
     * @param scope the scope of this setting
     */
    public Setting(String key, Setting<T> fallBackSetting, Function<String, T> parser, boolean dynamic, Scope scope) {
        this(key, fallBackSetting::getRaw, parser, dynamic, scope);
    }

    /**
     * Returns the settings key or a prefix if this setting is a group setting.
     * <b>Note: this method should not be used to retrieve a value from a {@link Settings} object.
     * Use {@link #get(Settings)} instead</b>
     *
     * @see #isGroupSetting()
     */
    public final String getKey() {
        return key.toString();
    }

    /**
     * Returns the original representation of a setting key.
     */
    public final Key getRawKey() {
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
     * Returns the default value string representation for this setting.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    public final String getDefaultRaw(Settings settings) {
        return defaultValue.apply(settings);
    }

    /**
     * Returns the default value for this setting.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    public final T getDefault(Settings settings) {
        return parser.apply(getDefaultRaw(settings));
    }

    /**
     * Returns <code>true</code> iff this setting is present in the given settings object. Otherwise <code>false</code>
     */
    public boolean exists(Settings settings) {
        return settings.get(getKey()) != null;
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
        return settings.get(getKey(), defaultValue.apply(settings));
    }

    /**
     * Returns <code>true</code> iff the given key matches the settings key or if this setting is a group setting if the
     * given key is part of the settings group.
     * @see #isGroupSetting()
     */
    public final boolean match(String toTest) {
        return key.match(toTest);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("key", key.toString());
        builder.field("type", scope.name());
        builder.field("dynamic", dynamic);
        builder.field("is_group_setting", isGroupSetting());
        builder.field("default", defaultValue.apply(Settings.EMPTY));
        builder.endObject();
        return builder;
    }

    /**
     * Returns the value for this setting but falls back to the second provided settings object
     */
    public final T get(Settings primary, Settings secondary) {
        if (exists(primary)) {
            return get(primary);
        }
        return get(secondary);
    }

    public Setting<T> getConcreteSetting(String key) {
        assert key.startsWith(this.getKey()) : "was " + key + " expected: " + getKey(); // we use startsWith here since the key might be foo.bar.0 if it's an array
        return this;
    }

    /**
     * The settings scope - settings can either be cluster settings or per index settings.
     */
    public enum Scope {
        CLUSTER,
        INDEX;
    }

    /**
     * Build a new updater with a noop validator.
     */
    final AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, ESLogger logger) {
        return newUpdater(consumer, logger, (s) -> {});
    }

    /**
     * Build the updater responsible for validating new values, logging the new
     * value, and eventually setting the value where it belongs.
     */
    AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, ESLogger logger, Consumer<T> validator) {
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
    static <A, B> AbstractScopedSettings.SettingUpdater<Tuple<A, B>> compoundUpdater(final BiConsumer<A,B> consumer, final Setting<A> aSetting, final Setting<B> bSetting, ESLogger logger) {
        final AbstractScopedSettings.SettingUpdater<A> aSettingUpdater = aSetting.newUpdater(null, logger);
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


    private final class Updater implements AbstractScopedSettings.SettingUpdater<T> {
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
        public final void apply(T value, Settings current, Settings previous) {
            logger.info("updating [{}] from [{}] to [{}]", key, getRaw(previous), getRaw(current));
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

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, int maxValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, maxValue, key), dynamic, scope);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, key), dynamic, scope);
    }

    public static Setting<Long> longSetting(String key, long defaultValue, long minValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Long.toString(defaultValue), (s) -> parseLong(s, minValue, key), dynamic, scope);
    }

    public static Setting<String> simpleString(String key, boolean dynamic, Scope scope) {
        return new Setting<>(key, "", Function.identity(), dynamic, scope);
    }

    public static int parseInt(String s, int minValue, String key) {
        return parseInt(s, minValue, Integer.MAX_VALUE, key);
    }

    public static int parseInt(String s, int minValue, int maxValue, String key) {
        int value = Integer.parseInt(s);
        if (value < minValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        if (value > maxValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be =< " + maxValue);
        }
        return value;
    }

    public static long parseLong(String s, long minValue, String key) {
        long value = Long.parseLong(s);
        if (value < minValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        return value;
    }

    public static TimeValue parseTimeValue(String s, TimeValue minValue, String key) {
        TimeValue timeValue = TimeValue.parseTimeValue(s, null, key);
        if (timeValue.millis() < minValue.millis()) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        return timeValue;
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, boolean dynamic, Scope scope) {
        return intSetting(key, defaultValue, Integer.MIN_VALUE, dynamic, scope);
    }

    public static Setting<Boolean> boolSetting(String key, boolean defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> Boolean.toString(defaultValue), Booleans::parseBooleanExact, dynamic, scope);
    }

    public static Setting<Boolean> boolSetting(String key, Setting<Boolean> fallbackSetting, boolean dynamic, Scope scope) {
        return new Setting<>(key, fallbackSetting, Booleans::parseBooleanExact, dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, String percentage, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> percentage, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue value, boolean dynamic, Scope scope) {
        return byteSizeSetting(key, (s) -> value.toString(), dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Setting<ByteSizeValue> fallbackSettings, boolean dynamic, Scope scope) {
        return byteSizeSetting(key, fallbackSettings::getRaw, dynamic, scope);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Function<Settings, String> defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, defaultValue, (s) -> ByteSizeValue.parseBytesSizeValue(s, key), dynamic, scope);
    }

    public static Setting<TimeValue> positiveTimeSetting(String key, TimeValue defaultValue, boolean dynamic, Scope scope) {
        return timeSetting(key, defaultValue, TimeValue.timeValueMillis(0), dynamic, scope);
    }

    public static <T> Setting<List<T>> listSetting(String key, List<String> defaultStringValue, Function<String, T> singleValueParser, boolean dynamic, Scope scope) {
        return listSetting(key, (s) -> defaultStringValue, singleValueParser, dynamic, scope);
    }

    public static <T> Setting<List<T>> listSetting(String key, Setting<List<T>> fallbackSetting, Function<String, T> singleValueParser, boolean dynamic, Scope scope) {
        return listSetting(key, (s) -> parseableStringToList(fallbackSetting.getRaw(s)), singleValueParser, dynamic, scope);
    }

    public static <T> Setting<List<T>> listSetting(String key, Function<Settings, List<String>> defaultStringValue, Function<String, T> singleValueParser, boolean dynamic, Scope scope) {
        Function<String, List<T>> parser = (s) ->
                parseableStringToList(s).stream().map(singleValueParser).collect(Collectors.toList());

        return new Setting<List<T>>(new ListKey(key), (s) -> arrayToParsableString(defaultStringValue.apply(s).toArray(Strings.EMPTY_ARRAY)), parser, dynamic, scope) {
            @Override
            public String getRaw(Settings settings) {
                String[] array = settings.getAsArray(getKey(), null);
                return array == null ? defaultValue.apply(settings) : arrayToParsableString(array);
            }

            @Override
            boolean hasComplexMatcher() {
                return true;
            }
        };
    }

    private static List<String> parseableStringToList(String parsableString) {
        try (XContentParser xContentParser = XContentType.JSON.xContent().createParser(parsableString)) {
            XContentParser.Token token = xContentParser.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("expected START_ARRAY but got " + token);
            }
            ArrayList<String> list = new ArrayList<>();
            while ((token = xContentParser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.VALUE_STRING) {
                    throw new IllegalArgumentException("expected VALUE_STRING but got " + token);
                }
                list.add(xContentParser.text());
            }
            return list;
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse array", e);
        }
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
        return groupSetting(key, dynamic, scope, (s) -> {});
    }
    public static Setting<Settings> groupSetting(String key, boolean dynamic, Scope scope, Consumer<Settings> validator) {
        return new Setting<Settings>(new GroupKey(key), (s) -> "", (s) -> null, dynamic, scope) {
            @Override
            public boolean isGroupSetting() {
                return true;
            }

            @Override
            public String getRaw(Settings settings) {
                Settings subSettings = get(settings);
                try {
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    subSettings.toXContent(builder, EMPTY_PARAMS);
                    builder.endObject();
                    return builder.string();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Settings get(Settings settings) {
                Settings byPrefix = settings.getByPrefix(getKey());
                validator.accept(byPrefix);
                return byPrefix;
            }

            @Override
            public boolean exists(Settings settings) {
                for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                    if (entry.getKey().startsWith(key)) {
                        return true;
                    }
                }
                return false;
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
                        logger.info("updating [{}] from [{}] to [{}]", key, getRaw(previous), getRaw(current));
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
        return new Setting<>(key, defaultValue, (s) -> parseTimeValue(s, minValue, key), dynamic, scope);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, TimeValue minValue, boolean dynamic, Scope scope) {
        return timeSetting(key, (s) -> defaultValue.getStringRep(), minValue, dynamic, scope);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, boolean dynamic, Scope scope) {
        return new Setting<>(key, (s) -> defaultValue.toString(), (s) -> TimeValue.parseTimeValue(s, key), dynamic, scope);
    }

    public static Setting<TimeValue> timeSetting(String key, Setting<TimeValue> fallbackSetting, boolean dynamic, Scope scope) {
        return new Setting<>(key, fallbackSetting::getRaw, (s) -> TimeValue.parseTimeValue(s, key), dynamic, scope);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Setting<?> setting = (Setting<?>) o;
        return Objects.equals(key, setting.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    /**
     * This setting type allows to validate settings that have the same type and a common prefix. For instance feature.${type}=[true|false]
     * can easily be added with this setting. Yet, prefix key settings don't support updaters out of the box unless
     * {@link #getConcreteSetting(String)} is used to pull the updater.
     */
    public static <T> Setting<T> prefixKeySetting(String prefix, String defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        return affixKeySetting(AffixKey.withPrefix(prefix), (s) -> defaultValue, parser, dynamic, scope);
    }

    /**
     * This setting type allows to validate settings that have the same type and a common prefix and suffix. For instance
     * storage.${backend}.enable=[true|false] can easily be added with this setting. Yet, adfix key settings don't support updaters
     * out of the box unless {@link #getConcreteSetting(String)} is used to pull the updater.
     */
    public static <T> Setting<T> adfixKeySetting(String prefix, String suffix, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        return affixKeySetting(AffixKey.withAdfix(prefix, suffix), defaultValue, parser, dynamic, scope);
    }

    public static <T> Setting<T> adfixKeySetting(String prefix, String suffix, String defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        return adfixKeySetting(prefix, suffix, (s) -> defaultValue, parser, dynamic, scope);
    }

    public static <T> Setting<T> affixKeySetting(AffixKey key, Function<Settings, String> defaultValue, Function<String, T> parser, boolean dynamic, Scope scope) {
        return new Setting<T>(key, defaultValue, parser, dynamic, scope) {

            @Override
            boolean isGroupSetting() {
                return true;
            }

            @Override
            AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, ESLogger logger, Consumer<T> validator) {
                throw new UnsupportedOperationException("Affix settings can't be updated. Use #getConcreteSetting for updating.");
            }

            @Override
            public Setting<T> getConcreteSetting(String key) {
                if (match(key)) {
                    return new Setting<>(key, defaultValue, parser, dynamic, scope);
                } else {
                    throw new IllegalArgumentException("key [" + key + "] must match [" + getKey() + "] but didn't.");
                }
            }
        };
    }


    public interface Key {
        boolean match(String key);
    }

    public static class SimpleKey implements Key {
        protected final String key;

        public SimpleKey(String key) {
            this.key = key;
        }

        @Override
        public boolean match(String key) {
            return this.key.equals(key);
        }

        @Override
        public String toString() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleKey simpleKey = (SimpleKey) o;
            return Objects.equals(key, simpleKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    public static final class GroupKey extends SimpleKey {
        public GroupKey(String key) {
            super(key);
            if (key.endsWith(".") == false) {
                throw new IllegalArgumentException("key must end with a '.'");
            }
        }

        @Override
        public boolean match(String toTest) {
            return Regex.simpleMatch(key + "*", toTest);
        }
    }

    public static final class ListKey extends SimpleKey {
        private final Pattern pattern;

        public ListKey(String key) {
            super(key);
            this.pattern = Pattern.compile(Pattern.quote(key) + "(\\.\\d+)?");
        }

        @Override
        public boolean match(String toTest) {
            return pattern.matcher(toTest).matches();
        }
    }

    public static final class AffixKey implements Key {
        public static AffixKey withPrefix(String prefix) {
            return new AffixKey(prefix, null);
        }

        public static AffixKey withAdfix(String prefix, String suffix) {
            return new AffixKey(prefix, suffix);
        }

        private final String prefix;
        private final String suffix;

        public AffixKey(String prefix, String suffix) {
            assert prefix != null || suffix != null: "Either prefix or suffix must be non-null";
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public boolean match(String key) {
            boolean match = true;
            if (prefix != null) {
                match = key.startsWith(prefix);
            }
            if (suffix != null) {
                match = match && key.endsWith(suffix);
            }
            return match;
        }

        public SimpleKey toConcreteKey(String missingPart) {
            StringBuilder key = new StringBuilder();
            if (prefix != null) {
                key.append(prefix);
            }
            key.append(missingPart);
            if (suffix != null) {
                key.append(".");
                key.append(suffix);
            }
            return new SimpleKey(key.toString());
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (prefix != null) {
                sb.append(prefix);
            }
            if (suffix != null) {
                sb.append("*");
                sb.append(suffix);
                sb.append(".");
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AffixKey that = (AffixKey) o;
            return Objects.equals(prefix, that.prefix) &&
                Objects.equals(suffix, that.suffix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, suffix);
        }
    }
}
