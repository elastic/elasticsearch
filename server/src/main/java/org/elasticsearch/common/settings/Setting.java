/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A setting. Encapsulates typical stuff like default value, parsing, and scope.
 * Some (SettingsProperty.Dynamic) can by modified at run time using the API.
 * All settings inside elasticsearch or in any of the plugins should use this type-safe and generic settings infrastructure
 * together with {@link AbstractScopedSettings}. This class contains several utility methods that makes it straight forward
 * to add settings for the majority of the cases. For instance a simple boolean settings can be defined like this:
 * <pre>{@code
 * public static final Setting<Boolean>; MY_BOOLEAN = Setting.boolSetting("my.bool.setting", true, SettingsProperty.NodeScope);}
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
 * public static final Setting<Color> MY_BOOLEAN =
 *     new Setting<>("my.color.setting", Color.RED.toString(), Color::valueOf, SettingsProperty.NodeScope);
 * }
 * </pre>
 */
public class Setting<T> implements ToXContentObject {

    public enum Property {
        /**
         * should be filtered in some api (mask password/credentials)
         */
        Filtered,

        /**
         * iff this setting can be dynamically updateable
         */
        Dynamic,

        /**
         * Operator only Dynamic setting
         */
        OperatorDynamic,

        /**
         * mark this setting as final, not updateable even when the context is not dynamic
         * ie. Setting this property on an index scoped setting will fail update when the index is closed
         */
        Final,

        /**
         * mark this setting as deprecated
         */
        Deprecated,

        /**
         * Node scope
         */
        NodeScope,

        /**
         * Secure setting values equal on all nodes
         */
        Consistent,

        /**
         * Index scope
         */
        IndexScope,

        /**
         * Mark this setting as not copyable during an index resize (shrink or split). This property can only be applied to settings that
         * also have {@link Property#IndexScope}.
         */
        NotCopyableOnResize,

        /**
         * Indicates an index-level setting that is managed internally. Such a setting can only be added to an index on index creation but
         * can not be updated via the update API.
         */
        InternalIndex,

        /**
         * Indicates an index-level setting that is privately managed. Such a setting can not even be set on index creation.
         */
        PrivateIndex
    }

    private final Key key;
    protected final Function<Settings, String> defaultValue;
    @Nullable
    private final Setting<T> fallbackSetting;
    private final Function<String, T> parser;
    private final Validator<T> validator;
    private final EnumSet<Property> properties;

    private static final EnumSet<Property> EMPTY_PROPERTIES = EnumSet.noneOf(Property.class);

    private Setting(Key key, @Nullable Setting<T> fallbackSetting, Function<Settings, String> defaultValue, Function<String, T> parser,
                    Validator<T> validator, Property... properties) {
        assert this instanceof SecureSetting || this.isGroupSetting() || parser.apply(defaultValue.apply(Settings.EMPTY)) != null
               : "parser returned null";
        this.key = key;
        this.fallbackSetting = fallbackSetting;
        this.defaultValue = defaultValue;
        this.parser = parser;
        this.validator = validator;
        if (properties == null) {
            throw new IllegalArgumentException("properties cannot be null for setting [" + key + "]");
        }
        if (properties.length == 0) {
            this.properties = EMPTY_PROPERTIES;
        } else {
            final EnumSet<Property> propertiesAsSet = EnumSet.copyOf(Arrays.asList(properties));
            if ((propertiesAsSet.contains(Property.Dynamic) || propertiesAsSet.contains(Property.OperatorDynamic))
                && propertiesAsSet.contains(Property.Final)) {
                throw new IllegalArgumentException("final setting [" + key + "] cannot be dynamic");
            }
            if (propertiesAsSet.contains(Property.Dynamic) && propertiesAsSet.contains(Property.OperatorDynamic)) {
                throw new IllegalArgumentException("setting [" + key + "] cannot be both dynamic and operator dynamic");
            }
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.NotCopyableOnResize);
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.InternalIndex);
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.PrivateIndex);
            checkPropertyRequiresNodeScope(propertiesAsSet, Property.Consistent);
            this.properties = propertiesAsSet;
        }
    }

    private void checkPropertyRequiresIndexScope(final EnumSet<Property> properties, final Property property) {
        if (properties.contains(property) && properties.contains(Property.IndexScope) == false) {
            throw new IllegalArgumentException("non-index-scoped setting [" + key + "] can not have property [" + property + "]");
        }
    }

    private void checkPropertyRequiresNodeScope(final EnumSet<Property> properties, final Property property) {
        if (properties.contains(property) && properties.contains(Property.NodeScope) == false) {
            throw new IllegalArgumentException("non-node-scoped setting [" + key + "] can not have property [" + property + "]");
        }
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param defaultValue a default value function that returns the default values string representation.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(Key key, Function<Settings, String> defaultValue, Function<String, T> parser, Property... properties) {
        this(key, defaultValue, parser, v -> {}, properties);
    }

    /**
     * Creates a new {@code Setting} instance.
     *
     * @param key          the settings key for this setting
     * @param defaultValue a default value function that results a string representation of the default value
     * @param parser       a parser that parses a string representation into the concrete type for this setting
     * @param validator    a {@link Validator} for validating this setting
     * @param properties   properties for this setting
     */
    public Setting(
            Key key, Function<Settings, String> defaultValue, Function<String, T> parser, Validator<T> validator, Property... properties) {
        this(key, null, defaultValue, parser, validator, properties);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param defaultValue a default value.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(String key, String defaultValue, Function<String, T> parser, Property... properties) {
        this(key, s -> defaultValue, parser, properties);
    }

    /**
     * Creates a new {@code Setting} instance.
     *
     * @param key          the settings key for this setting
     * @param defaultValue a default value function that results a string representation of the default value
     * @param parser       a parser that parses a string representation into the concrete type for this setting
     * @param validator    a {@link Validator} for validating this setting
     * @param properties   properties for this setting
     */
    public Setting(String key, String defaultValue, Function<String, T> parser, Validator<T> validator, Property... properties) {
        this(new SimpleKey(key), s -> defaultValue, parser, validator, properties);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param defaultValue a default value function that returns the default values string representation.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(String key, Function<Settings, String> defaultValue, Function<String, T> parser, Property... properties) {
        this(new SimpleKey(key), defaultValue, parser, properties);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param fallbackSetting a setting who's value to fallback on if this setting is not defined
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param validator a {@link Validator} for validating this setting
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(String key, Setting<T> fallbackSetting, Function<String, T> parser, Validator<T> validator, Property... properties) {
        this(new SimpleKey(key), fallbackSetting, fallbackSetting::getRaw, parser, validator, properties);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param fallbackSetting a setting who's value to fallback on if this setting is not defined
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(Key key, Setting<T> fallbackSetting, Function<String, T> parser, Property... properties) {
        this(key, fallbackSetting, fallbackSetting::getRaw, parser, v -> {}, properties);
    }

    /**
     * Creates a new Setting instance
     * @param key the settings key for this setting.
     * @param fallBackSetting a setting to fall back to if the current setting is not set.
     * @param parser a parser that parses the string rep into a complex datatype.
     * @param properties properties for this setting like scope, filtering...
     */
    public Setting(String key, Setting<T> fallBackSetting, Function<String, T> parser, Property... properties) {
        this(new SimpleKey(key), fallBackSetting, parser, properties);
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
     * Returns <code>true</code> if this setting is dynamically updateable, otherwise <code>false</code>
     */
    public final boolean isDynamic() {
        return properties.contains(Property.Dynamic) || properties.contains(Property.OperatorDynamic);
    }

    /**
     * Returns <code>true</code> if this setting is dynamically updateable by operators, otherwise <code>false</code>
     */
    public final boolean isOperatorOnly() {
        return properties.contains(Property.OperatorDynamic);
    }

    /**
     * Returns <code>true</code> if this setting is final, otherwise <code>false</code>
     */
    public final boolean isFinal() {
        return properties.contains(Property.Final);
    }

    public final boolean isInternalIndex() {
        return properties.contains(Property.InternalIndex);
    }

    public final boolean isPrivateIndex() {
        return properties.contains(Property.PrivateIndex);
    }

    /**
     * Checks whether this is a secure setting.
     * @param settings used to check whether this setting is secure
     * @return whether this is a secure setting.
     */
    public final boolean isSecure(Settings settings) {
        final SecureSettings secureSettings = settings.getSecureSettings();
        return secureSettings != null && secureSettings.getSettingNames().contains(getKey());
    }

    /**
     * Returns the setting properties
     * @see Property
     */
    public EnumSet<Property> getProperties() {
        return properties;
    }

    /**
     * Returns <code>true</code> if this setting must be filtered, otherwise <code>false</code>
     */
    public boolean isFiltered() {
        return properties.contains(Property.Filtered);
    }

    /**
     * Returns <code>true</code> if this setting has a node scope, otherwise <code>false</code>
     */
    public boolean hasNodeScope() {
        return properties.contains(Property.NodeScope);
    }

    /**
     * Returns <code>true</code> if this setting's value can be checked for equality across all nodes. Only {@link SecureSetting} instances
     * may have this qualifier.
     */
    public boolean isConsistent() {
        return properties.contains(Property.Consistent);
    }

    /**
     * Returns <code>true</code> if this setting has an index scope, otherwise <code>false</code>
     */
    public boolean hasIndexScope() {
        return properties.contains(Property.IndexScope);
    }

    /**
     * Returns <code>true</code> if this setting is deprecated, otherwise <code>false</code>
     */
    public boolean isDeprecated() {
        return properties.contains(Property.Deprecated);
    }

    /**
     * Returns <code>true</code> iff this setting is a group setting. Group settings represent a set of settings rather than a single value.
     * The key, see {@link #getKey()}, in contrast to non-group settings is a prefix like {@code cluster.store.} that matches all settings
     * with this prefix.
     */
    boolean isGroupSetting() {
        return false;
    }


    final boolean isListSetting() {
        return this instanceof ListSetting;
    }

    boolean hasComplexMatcher() {
        return isGroupSetting();
    }

    /**
     * Validate the current setting value only without dependencies with {@link Setting.Validator#validate(Object)}.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    void validateWithoutDependencies(Settings settings) {
        validator.validate(get(settings, false));
    }

    /**
     * Returns the default value string representation for this setting.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    public String getDefaultRaw(Settings settings) {
        return defaultValue.apply(settings);
    }

    /**
     * Returns the default value for this setting.
     * @param settings a settings object for settings that has a default value depending on another setting if available
     */
    public T getDefault(Settings settings) {
        return parser.apply(getDefaultRaw(settings));
    }

    /**
     * Returns true if and only if this setting is present in the given settings instance. Note that fallback settings are excluded.
     *
     * @param settings the settings
     * @return true if the setting is present in the given settings instance, otherwise false
     */
    public boolean exists(final Settings settings) {
        return exists(settings.keySet());
    }

    public boolean exists(final Settings.Builder builder) {
        return exists(builder.keys());
    }

    private boolean exists(final Set<String> keys) {
        return keys.contains(getKey());
    }

    /**
     * Returns true if and only if this setting including fallback settings is present in the given settings instance.
     *
     * @param settings the settings
     * @return true if the setting including fallback settings is present in the given settings instance, otherwise false
     */
    public boolean existsOrFallbackExists(final Settings settings) {
        return settings.keySet().contains(getKey()) || (fallbackSetting != null && fallbackSetting.existsOrFallbackExists(settings));
    }

    /**
     * Returns the settings value. If the setting is not present in the given settings object the default value is returned
     * instead.
     */
    public T get(Settings settings) {
        return get(settings, true);
    }

    private T get(Settings settings, boolean validate) {
        String value = getRaw(settings);
        try {
            T parsed = parser.apply(value);
            if (validate) {
                final Iterator<Setting<?>> it = validator.settings();
                final Map<Setting<?>, Object> map;
                if (it.hasNext()) {
                    map = new HashMap<>();
                    while (it.hasNext()) {
                        final Setting<?> setting = it.next();
                        if (setting instanceof AffixSetting) {
                            // Collect all possible concrete settings
                            AffixSetting<?> as = ((AffixSetting<?>)setting);
                            for (String ns : as.getNamespaces(settings)) {
                                Setting<?> s = as.getConcreteSettingForNamespace(ns);
                                map.put(s, s.get(settings, false));
                            }
                        } else {
                            map.put(setting, setting.get(settings, false)); // we have to disable validation or we will stack overflow
                        }
                    }
                } else {
                    map = Collections.emptyMap();
                }
                validator.validate(parsed);
                validator.validate(parsed, map);
                validator.validate(parsed, map, exists(settings));
            }
            return parsed;
        } catch (ElasticsearchParseException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        } catch (NumberFormatException ex) {
            String err = "Failed to parse value" + (isFiltered() ? "" : " [" + value + "]") + " for setting [" + getKey() + "]";
            throw new IllegalArgumentException(err, ex);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception t) {
            String err = "Failed to parse value" + (isFiltered() ? "" : " [" + value + "]") + " for setting [" + getKey() + "]";
            throw new IllegalArgumentException(err, t);
        }
    }

    /**
     * Add this setting to the builder if it doesn't exists in the source settings.
     * The value added to the builder is taken from the given default settings object.
     * @param builder the settings builder to fill the diff into
     * @param source the source settings object to diff
     * @param defaultSettings the default settings object to diff against
     */
    public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
        if (exists(source) == false) {
            if (exists(defaultSettings)) {
                // If the setting is only in the defaults, use the value from the defaults
                builder.put(getKey(), getRaw(defaultSettings));
            } else {
                // If the setting is in neither `source` nor `default`, get the value
                // from `source` as it may depend on the value of other settings
                builder.put(getKey(), getRaw(source));
            }
        }
    }

    /**
     * Returns the raw (string) settings value. If the setting is not present in the given settings object the default value is returned
     * instead. This is useful if the value can't be parsed due to an invalid value to access the actual value.
     */
    private String getRaw(final Settings settings) {
        checkDeprecation(settings);
        return innerGetRaw(settings);
    }

    /**
     * The underlying implementation for {@link #getRaw(Settings)}. Setting specializations can override this as needed to convert the
     * actual settings value to raw strings.
     *
     * @param settings the settings instance
     * @return the raw string representation of the setting value
     */
    String innerGetRaw(final Settings settings) {
        if (this.isSecure(settings)) {
            throw new IllegalArgumentException("Setting [" + getKey() + "] is a non-secure setting" +
                " and must be stored inside elasticsearch.yml, but was found inside the Elasticsearch keystore");
        }
        return settings.get(getKey(), defaultValue.apply(settings));
    }

    /** Logs a deprecation warning if the setting is deprecated and used. */
    void checkDeprecation(Settings settings) {
        // They're using the setting, so we need to tell them to stop
        if (this.isDeprecated() && this.exists(settings)) {
            // It would be convenient to show its replacement key, but replacement is often not so simple
            final String key = getKey();

            DeprecationCategory category = this.isSecure(settings) ? DeprecationCategory.SECURITY : DeprecationCategory.SETTINGS;

            Settings.DeprecationLoggerHolder.deprecationLogger
                .deprecate(category, key, "[{}] setting was deprecated in Elasticsearch and will be removed in a future release! "
                    + "See the breaking changes documentation for the next major version.", key);
        }
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
        builder.field("properties", properties);
        builder.field("is_group_setting", isGroupSetting());
        builder.field("default", defaultValue.apply(Settings.EMPTY));
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    /**
     * Returns the value for this setting but falls back to the second provided settings object
     */
    public final T get(Settings primary, Settings secondary) {
        if (exists(primary)) {
            return get(primary);
        }
        if (exists(secondary)) {
            return get(secondary);
        }
        if (fallbackSetting == null) {
            return get(primary);
        }
        if (fallbackSetting.exists(primary)) {
            return fallbackSetting.get(primary);
        }
        return fallbackSetting.get(secondary);
    }

    public Setting<T> getConcreteSetting(String key) {
        // we use startsWith here since the key might be foo.bar.0 if it's an array
        assert key.startsWith(this.getKey()) : "was " + key + " expected: " + getKey();
        return this;
    }

    /**
     * Allows a setting to declare a dependency on another setting being set. Optionally, a setting can validate the value of the dependent
     * setting.
     */
    public interface SettingDependency {

        /**
         * The setting to declare a dependency on.
         *
         * @return the setting
         */
        Setting<?> getSetting();

        /**
         * Validates the dependent setting value.
         *
         * @param key        the key for this setting
         * @param value      the value of this setting
         * @param dependency the value of the dependent setting
         */
        default void validate(String key, Object value, Object dependency) {

        }

    }

    /**
     * Returns a set of settings that are required at validation time. Unless all of the dependencies are present in the settings
     * object validation of setting must fail.
     */
    public Set<SettingDependency> getSettingsDependencies(final String key) {
        return Collections.emptySet();
    }

    /**
     * Build a new updater with a noop validator.
     */
    final AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, Logger logger) {
        return newUpdater(consumer, logger, (s) -> {});
    }

    /**
     * Build the updater responsible for validating new values, logging the new
     * value, and eventually setting the value where it belongs.
     */
    AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, Logger logger, Consumer<T> validator) {
        if (isDynamic()) {
            return new Updater(consumer, logger, validator);
        } else {
            throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
        }
    }

    /**
     * Updates settings that depend on each other.
     * See {@link AbstractScopedSettings#addSettingsUpdateConsumer(Setting, Setting, BiConsumer)} and its usage for details.
     */
    static <A, B> AbstractScopedSettings.SettingUpdater<Tuple<A, B>> compoundUpdater(final BiConsumer<A, B> consumer,
            final BiConsumer<A, B> validator, final Setting<A> aSetting, final Setting<B> bSetting, Logger logger) {
        final AbstractScopedSettings.SettingUpdater<A> aSettingUpdater = aSetting.newUpdater(null, logger);
        final AbstractScopedSettings.SettingUpdater<B> bSettingUpdater = bSetting.newUpdater(null, logger);
        return new AbstractScopedSettings.SettingUpdater<Tuple<A, B>>() {
            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                return aSettingUpdater.hasChanged(current, previous) || bSettingUpdater.hasChanged(current, previous);
            }

            @Override
            public Tuple<A, B> getValue(Settings current, Settings previous) {
                A valueA = aSettingUpdater.getValue(current, previous);
                B valueB = bSettingUpdater.getValue(current, previous);
                validator.accept(valueA, valueB);
                return new Tuple<>(valueA, valueB);
            }

            @Override
            public void apply(Tuple<A, B> value, Settings current, Settings previous) {
                if (aSettingUpdater.hasChanged(current, previous)) {
                    logSettingUpdate(aSetting, current, previous, logger);
                }
                if (bSettingUpdater.hasChanged(current, previous)) {
                    logSettingUpdate(bSetting, current, previous, logger);
                }
                consumer.accept(value.v1(), value.v2());
            }

            @Override
            public String toString() {
                return "CompoundUpdater for: " + aSettingUpdater + " and " + bSettingUpdater;
            }
        };
    }

    static AbstractScopedSettings.SettingUpdater<Settings> groupedSettingsUpdater(Consumer<Settings> consumer,
                                                                                  final List<? extends Setting<?>> configuredSettings) {
        return groupedSettingsUpdater(consumer, configuredSettings, (v) -> {});
    }

    static AbstractScopedSettings.SettingUpdater<Settings> groupedSettingsUpdater(Consumer<Settings> consumer,
                                                                                  final List<? extends Setting<?>> configuredSettings,
                                                                                  Consumer<Settings> validator) {
        return new AbstractScopedSettings.SettingUpdater<Settings>() {

            private Settings get(Settings settings) {
                return settings.filter(s -> {
                    for (Setting<?> setting : configuredSettings) {
                        if (setting.key.match(s)) {
                            return true;
                        }
                    }
                    return false;
                });
            }

            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                Settings currentSettings = get(current);
                Settings previousSettings = get(previous);
                return currentSettings.equals(previousSettings) == false;
            }

            @Override
            public Settings getValue(Settings current, Settings previous) {
                validator.accept(current);
                return get(current);
            }

            @Override
            public void apply(Settings value, Settings current, Settings previous) {
                consumer.accept(value);
            }

            @Override
            public String toString() {
                return "Updater grouped: " + configuredSettings.stream().map(Setting::getKey).collect(Collectors.joining(", "));
            }
        };
    }

    /**
     * Allows an affix setting to declare a dependency on another affix setting.
     */
    public interface AffixSettingDependency extends SettingDependency {

        @Override
        AffixSetting<?> getSetting();

    }

    public static class AffixSetting<T> extends Setting<T> {
        private final AffixKey key;
        private final BiFunction<String, String, Setting<T>> delegateFactory;
        private final Set<AffixSettingDependency> dependencies;

        public AffixSetting(AffixKey key, Setting<T> delegate, BiFunction<String, String, Setting<T>> delegateFactory,
                            AffixSettingDependency... dependencies) {
            super(key, delegate.defaultValue, delegate.parser, delegate.properties.toArray(new Property[0]));
            this.key = key;
            this.delegateFactory = delegateFactory;
            this.dependencies = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(dependencies)));
        }

        boolean isGroupSetting() {
            return true;
        }

        private Stream<String> matchStream(Settings settings) {
            return settings.keySet().stream().filter(this::match).map(key::getConcreteString);
        }

        /**
         * Get the raw list of dependencies. This method is exposed for testing purposes and {@link #getSettingsDependencies(String)}
         * should be preferred for most all cases.
         * @return the raw list of dependencies for this setting
         */
        public Set<AffixSettingDependency> getDependencies() {
            return Collections.unmodifiableSet(dependencies);
        }

        @Override
        public Set<SettingDependency> getSettingsDependencies(String settingsKey) {
            if (dependencies.isEmpty()) {
                return Collections.emptySet();
            } else {
                String namespace = key.getNamespace(settingsKey);
                return dependencies.stream()
                    .map(s ->
                        new SettingDependency() {
                            @Override
                            public Setting<?> getSetting() {
                                return s.getSetting().getConcreteSettingForNamespace(namespace);
                            }

                            @Override
                            public void validate(final String key, final Object value, final Object dependency) {
                                s.validate(key, value, dependency);
                            };
                        })
                    .collect(Collectors.toSet());
            }
        }

        AbstractScopedSettings.SettingUpdater<Map<AbstractScopedSettings.SettingUpdater<T>, T>> newAffixUpdater(
            BiConsumer<String, T> consumer, Logger logger, BiConsumer<String, T> validator) {
            return new AbstractScopedSettings.SettingUpdater<Map<AbstractScopedSettings.SettingUpdater<T>, T>>() {

                @Override
                public boolean hasChanged(Settings current, Settings previous) {
                    return  Stream.concat(matchStream(current), matchStream(previous)).findAny().isPresent();
                }

                @Override
                public Map<AbstractScopedSettings.SettingUpdater<T>, T> getValue(Settings current, Settings previous) {
                    // we collect all concrete keys and then delegate to the actual setting for validation and settings extraction
                    final Map<AbstractScopedSettings.SettingUpdater<T>, T> result = new IdentityHashMap<>();
                    Stream.concat(matchStream(current), matchStream(previous)).distinct().forEach(aKey -> {
                        String namespace = key.getNamespace(aKey);
                        Setting<T> concreteSetting = getConcreteSetting(namespace, aKey);
                        AbstractScopedSettings.SettingUpdater<T> updater =
                            concreteSetting.newUpdater((v) -> consumer.accept(namespace, v), logger,
                                (v) -> validator.accept(namespace, v));
                        if (updater.hasChanged(current, previous)) {
                            // only the ones that have changed otherwise we might get too many updates
                            // the hasChanged above checks only if there are any changes
                            T value = updater.getValue(current, previous);
                            result.put(updater, value);
                        }
                    });
                    return result;
                }

                @Override
                public void apply(Map<AbstractScopedSettings.SettingUpdater<T>, T> value, Settings current, Settings previous) {
                    for (Map.Entry<AbstractScopedSettings.SettingUpdater<T>, T> entry : value.entrySet()) {
                        entry.getKey().apply(entry.getValue(), current, previous);
                    }
                }
            };
        }

        AbstractScopedSettings.SettingUpdater<Map<String, T>> newAffixMapUpdater(Consumer<Map<String, T>> consumer, Logger logger,
                                                                                 BiConsumer<String, T> validator) {
            return new AbstractScopedSettings.SettingUpdater<Map<String, T>>() {

                @Override
                public boolean hasChanged(Settings current, Settings previous) {
                    return current.filter(k -> match(k)).equals(previous.filter(k -> match(k))) == false;
                }

                @Override
                public Map<String, T> getValue(Settings current, Settings previous) {
                    // we collect all concrete keys and then delegate to the actual setting for validation and settings extraction
                    final Map<String, T> result = new IdentityHashMap<>();
                    Stream.concat(matchStream(current), matchStream(previous)).distinct().forEach(aKey -> {
                        String namespace = key.getNamespace(aKey);
                        Setting<T> concreteSetting = getConcreteSetting(namespace, aKey);
                        AbstractScopedSettings.SettingUpdater<T> updater =
                            concreteSetting.newUpdater((v) -> {}, logger, (v) -> validator.accept(namespace, v));
                        if (updater.hasChanged(current, previous)) {
                            // only the ones that have changed otherwise we might get too many updates
                            // the hasChanged above checks only if there are any changes
                            T value = updater.getValue(current, previous);
                            result.put(namespace, value);
                        }
                    });
                    return result;
                }

                @Override
                public void apply(Map<String, T> value, Settings current, Settings previous) {
                    consumer.accept(value);
                }
            };
        }

        @Override
        public T get(Settings settings) {
            throw new UnsupportedOperationException("affix settings can't return values" +
                " use #getConcreteSetting to obtain a concrete setting");
        }

        @Override
        public String innerGetRaw(final Settings settings) {
            throw new UnsupportedOperationException("affix settings can't return values" +
                " use #getConcreteSetting to obtain a concrete setting");
        }

        @Override
        public Setting<T> getConcreteSetting(String key) {
            if (match(key)) {
                String namespace = this.key.getNamespace(key);
                return delegateFactory.apply(namespace, key);
            } else {
                throw new IllegalArgumentException("key [" + key + "] must match [" + getKey() + "] but didn't.");
            }
        }

        private Setting<T> getConcreteSetting(String namespace, String key) {
            if (match(key)) {
                return delegateFactory.apply(namespace, key);
            } else {
                throw new IllegalArgumentException("key [" + key + "] must match [" + getKey() + "] but didn't.");
            }
        }

        /**
         * Get a setting with the given namespace filled in for prefix and suffix.
         */
        public Setting<T> getConcreteSettingForNamespace(String namespace) {
            String fullKey = key.toConcreteKey(namespace).toString();
            return getConcreteSetting(namespace, fullKey);
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            matchStream(defaultSettings).forEach((key) -> getConcreteSetting(key).diff(builder, source, defaultSettings));
        }

        /**
         * Returns the namespace for a concrete setting. Ie. an affix setting with prefix: {@code search.} and suffix: {@code username}
         * will return {@code remote} as a namespace for the setting {@code cluster.remote.username}
         */
        public String getNamespace(Setting<T> concreteSetting) {
            return key.getNamespace(concreteSetting.getKey());
        }

        /**
         * Returns a stream of all concrete setting instances for the given settings. AffixSetting is only a specification, concrete
         * settings depend on an actual set of setting keys.
         */
        public Stream<Setting<T>> getAllConcreteSettings(Settings settings) {
            return matchStream(settings).distinct().map(this::getConcreteSetting);
        }

        /**
         * Returns distinct namespaces for the given settings
         */
        public Set<String> getNamespaces(Settings settings) {
            return settings.keySet().stream().filter(this::match).map(key::getNamespace).collect(Collectors.toSet());
        }

        /**
         * Returns a map of all namespaces to it's values give the provided settings
         */
        public Map<String, T> getAsMap(Settings settings) {
            Map<String, T> map = new HashMap<>();
            matchStream(settings).distinct().forEach(key -> {
                String namespace = this.key.getNamespace(key);
                Setting<T> concreteSetting = getConcreteSetting(namespace, key);
                map.put(namespace, concreteSetting.get(settings));
            });
            return Collections.unmodifiableMap(map);
        }
    }

    /**
     * Represents a validator for a setting. The {@link #validate(Object)} method is invoked early in the update setting process with the
     * value of this setting for a fail-fast validation. Later on, the {@link #validate(Object, Map)} and
     * {@link #validate(Object, Map, boolean)} methods are invoked with the value of this setting and a map from the settings specified by
     * {@link #settings()}} to their values. All these values come from the same {@link Settings} instance.
     *
     * @param <T> the type of the {@link Setting}
     */
    @FunctionalInterface
    public interface Validator<T> {

        /**
         * Validate this setting's value in isolation.
         *
         * @param value the value of this setting
         */
        void validate(T value);

        /**
         * Validate this setting against its dependencies, specified by {@link #settings()}. The default implementation does nothing,
         * accepting any value as valid as long as it passes the validation in {@link #validate(Object)}.
         *
         * @param value    the value of this setting
         * @param settings a map from the settings specified by {@link #settings()}} to their values
         */
        default void validate(T value, Map<Setting<?>, Object> settings) {
        }

        /**
         * Validate this setting against its dependencies, specified by {@link #settings()}. This method allows validation logic
         * to evaluate whether the setting will be present in the {@link Settings} after the update. The default implementation
         * does nothing, accepting any value as valid as long as it passes the validation in {@link #validate(Object)}.
         *
         * @param value     the value of this setting
         * @param settings  a map from the settings specified by {@link #settings()}} to their values
         * @param isPresent boolean indicating if this setting is present
         */
        default void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
        }

        /**
         * The settings on which the validity of this setting depends. The values of the specified settings are passed to
         * {@link #validate(Object, Map)}. By default this returns an empty iterator, indicating that this setting does not depend on any
         * other settings.
         *
         * @return the settings on which the validity of this setting depends.
         */
        default Iterator<Setting<?>> settings() {
            return Collections.emptyIterator();
        }

    }

    private static class GroupSetting extends Setting<Settings> {
        private final String key;
        private final Consumer<Settings> validator;

        private GroupSetting(String key, Consumer<Settings> validator, Property... properties) {
            super(new GroupKey(key), (s) -> "", (s) -> null, properties);
            this.key = key;
            this.validator = validator;
        }

        @Override
        public boolean isGroupSetting() {
            return true;
        }

        @Override
        public String innerGetRaw(final Settings settings) {
            Settings subSettings = get(settings);
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                subSettings.toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return Strings.toString(builder);
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
            for (String settingsKey : settings.keySet()) {
                if (settingsKey.startsWith(key)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            Set<String> leftGroup = get(source).keySet();
            Settings defaultGroup = get(defaultSettings);

            builder.put(Settings.builder().put(defaultGroup.filter(k -> leftGroup.contains(k) == false), false)
                    .normalizePrefix(getKey()).build(), false);
        }

        @Override
        public AbstractScopedSettings.SettingUpdater<Settings> newUpdater(Consumer<Settings> consumer, Logger logger,
                                                                          Consumer<Settings> validator) {
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
                        String err = "illegal value can't update [" + key + "]" +
                            (isFiltered() ? "" : " from [" + previousSettings + "] to [" + currentSettings+ "]");
                        throw new IllegalArgumentException(err, e);
                    }
                    return currentSettings;
                }

                @Override
                public void apply(Settings value, Settings current, Settings previous) {
                    Setting.logSettingUpdate(GroupSetting.this, current, previous, logger);
                    consumer.accept(value);
                }

                @Override
                public String toString() {
                    return "Updater for: " + setting.toString();
                }
            };
        }
    }

    private final class Updater implements AbstractScopedSettings.SettingUpdater<T> {
        private final Consumer<T> consumer;
        private final Logger logger;
        private final Consumer<T> accept;

        Updater(Consumer<T> consumer, Logger logger, Consumer<T> accept) {
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
            try {
                T inst = get(current);
                accept.accept(inst);
                return inst;
            } catch (Exception | AssertionError e) {
                if (isFiltered()) {
                    throw new IllegalArgumentException("illegal value can't update [" + key + "]");
                } else {
                    throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + value + "] to [" +
                        newValue + "]", e);
                }
            }
        }

        @Override
        public void apply(T value, Settings current, Settings previous) {
            logSettingUpdate(Setting.this, current, previous, logger);
            consumer.accept(value);
        }
    }

    public static Setting<Version> versionSetting(final String key, final Version defaultValue, Property... properties) {
        return new Setting<>(key, s -> Integer.toString(defaultValue.id), s -> Version.fromId(Integer.parseInt(s)), properties);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), Float::parseFloat, properties);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, float minValue, Property... properties) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), (s) -> {
            float value = Float.parseFloat(s);
            if (value < minValue) {
                String err = "Failed to parse value" +
                    (isFiltered(properties) ? "" : " [" + s + "]") + " for setting [" + key + "] must be >= " + minValue;
                throw new IllegalArgumentException(err);
            }
            return value;
        }, properties);
    }

    private static boolean isFiltered(Property[] properties) {
        return properties != null && Arrays.asList(properties).contains(Property.Filtered);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, int maxValue, Property... properties) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue),
            (s) -> parseInt(s, minValue, maxValue, key, isFiltered(properties)), properties);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, Property... properties) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, key, isFiltered(properties)),
            properties);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, Validator<Integer> validator,
                                              Property... properties) {
        return new Setting<>(key, Integer.toString(defaultValue), (s) -> parseInt(s, minValue, key, isFiltered(properties)), validator,
            properties);
    }

    public static Setting<Integer> intSetting(String key, Setting<Integer> fallbackSetting, int minValue, Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> parseInt(s, minValue, key, isFiltered(properties)), properties);
    }

    public static Setting<Integer> intSetting(String key, Setting<Integer> fallbackSetting, int minValue, int maxValue,
                                              Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> parseInt(s, minValue, maxValue, key, isFiltered(properties)), properties);
    }

    public static Setting<Integer> intSetting(String key, Setting<Integer> fallbackSetting, int minValue, Validator<Integer> validator,
                                              Property... properties) {
        return new Setting<>(new SimpleKey(key), fallbackSetting, fallbackSetting::getRaw,
            (s) -> parseInt(s, minValue, key, isFiltered(properties)),validator, properties);
    }

    public static Setting<Long> longSetting(String key, long defaultValue, long minValue, Property... properties) {
        return new Setting<>(key, (s) -> Long.toString(defaultValue), (s) -> parseLong(s, minValue, key, isFiltered(properties)),
            properties);
    }

    public static Setting<String> simpleString(String key, Property... properties) {
        return new Setting<>(key, s -> "", Function.identity(), properties);
    }

    public static Setting<String> simpleString(String key, Validator<String> validator, Property... properties) {
        return new Setting<>(new SimpleKey(key), null, s -> "", Function.identity(), validator, properties);
    }

    public static Setting<String> simpleString(String key, Validator<String> validator, Setting<String> fallback, Property... properties) {
        return new Setting<>(new SimpleKey(key), fallback, fallback::getRaw, Function.identity(), validator, properties);
    }

    public static Setting<String> simpleString(String key, String defaultValue, Validator<String> validator, Property... properties) {
        validator.validate(defaultValue);
        return new Setting<>(new SimpleKey(key), null, s -> defaultValue, Function.identity(), validator, properties);
    }

    public static Setting<String> simpleString(String key, Setting<String> fallback, Property... properties) {
        return simpleString(key, fallback, Function.identity(), properties);
    }

    public static Setting<String> simpleString(
            final String key,
            final Setting<String> fallback,
            final Function<String, String> parser,
            final Property... properties) {
        return new Setting<>(key, fallback, parser, properties);
    }

    /**
     * Creates a new Setting instance with a String value
     *
     * @param key          the settings key for this setting.
     * @param defaultValue the default String value.
     * @param properties   properties for this setting like scope, filtering...
     * @return the Setting Object
     */
    public static Setting<String> simpleString(String key, String defaultValue, Property... properties) {
        return new Setting<>(key, s -> defaultValue, Function.identity(), properties);
    }

    public static int parseInt(String s, int minValue, String key) {
        return parseInt(s, minValue, Integer.MAX_VALUE, key, false);
    }

    public static int parseInt(String s, int minValue, String key, boolean isFiltered) {
        return parseInt(s, minValue, Integer.MAX_VALUE, key, isFiltered);
    }

    public static int parseInt(String s, int minValue, int maxValue, String key) {
        return parseInt(s, minValue, maxValue, key, false);
    }

    public static int parseInt(String s, int minValue, int maxValue, String key, boolean isFiltered) {
        int value = Integer.parseInt(s);
        if (value < minValue) {
            String err = "Failed to parse value" + (isFiltered ? "" : " [" + s + "]") + " for setting [" + key + "] must be >= " + minValue;
            throw new IllegalArgumentException(err);
        }
        if (value > maxValue) {
            String err = "Failed to parse value" + (isFiltered ? "" : " [" + s + "]") + " for setting [" + key + "] must be <= " + maxValue;
            throw new IllegalArgumentException(err);
        }
        return value;
    }

    public static long parseLong(String s, long minValue, String key) {
        return parseLong(s, minValue, key, false);
    }

    static long parseLong(String s, long minValue, String key, boolean isFiltered) {
        long value = Long.parseLong(s);
        if (value < minValue) {
            String err = "Failed to parse value" + (isFiltered ? "" : " [" + s + "]") + " for setting [" + key + "] must be >= " + minValue;
            throw new IllegalArgumentException(err);
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

    public static Setting<Integer> intSetting(String key, int defaultValue, Property... properties) {
        return intSetting(key, defaultValue, Integer.MIN_VALUE, properties);
    }

    public static Setting<Boolean> boolSetting(String key, boolean defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> Boolean.toString(defaultValue), b -> parseBoolean(b, key, isFiltered(properties)), properties);
    }

    public static Setting<Boolean> boolSetting(String key, Setting<Boolean> fallbackSetting, Property... properties) {
        return new Setting<>(key, fallbackSetting, b -> parseBoolean(b, key, isFiltered(properties)), properties);
    }

    public static Setting<Boolean> boolSetting(String key, Setting<Boolean> fallbackSetting, Validator<Boolean> validator,
                                               Property... properties) {
        return new Setting<>(new SimpleKey(key), fallbackSetting, fallbackSetting::getRaw, b -> parseBoolean(b, key,
            isFiltered(properties)), validator, properties);
    }

    public static Setting<Boolean> boolSetting(String key, boolean defaultValue, Validator<Boolean> validator, Property... properties) {
        return new Setting<>(key, Boolean.toString(defaultValue), b -> parseBoolean(b, key, isFiltered(properties)), validator, properties);
    }

    public static Setting<Boolean> boolSetting(String key, Function<Settings, String> defaultValueFn, Property... properties) {
        return new Setting<>(key, defaultValueFn, b -> parseBoolean(b, key, isFiltered(properties)), properties);
    }

    static boolean parseBoolean(String b, String key, boolean isFiltered) {
        try {
            return Booleans.parseBoolean(b);
        } catch (IllegalArgumentException ex) {
            if (isFiltered) {
                throw new IllegalArgumentException("Failed to parse value for setting [" + key + "]");
            } else {
                throw ex;
            }
        }
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue value, Property... properties) {
        return byteSizeSetting(key, (s) -> value.toString(), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Setting<ByteSizeValue> fallbackSetting,
                                                         Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> ByteSizeValue.parseBytesSizeValue(s, key), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Function<Settings, String> defaultValue,
                                                         Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> ByteSizeValue.parseBytesSizeValue(s, key), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue defaultValue, ByteSizeValue minValue,
                                                         ByteSizeValue maxValue, Property... properties) {
        return byteSizeSetting(key, (s) -> defaultValue.getStringRep(), minValue, maxValue, properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Function<Settings, String> defaultValue,
                                                         ByteSizeValue minValue, ByteSizeValue maxValue,
                                                         Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> parseByteSize(s, minValue, maxValue, key), properties);
    }

    public static ByteSizeValue parseByteSize(String s, ByteSizeValue minValue, ByteSizeValue maxValue, String key) {
        ByteSizeValue value = ByteSizeValue.parseBytesSizeValue(s, key);
        if (value.getBytes() < minValue.getBytes()) {
            final String message = String.format(
                    Locale.ROOT,
                    "failed to parse value [%s] for setting [%s], must be >= [%s]",
                    s,
                    key,
                    minValue.getStringRep());
            throw new IllegalArgumentException(message);
        }
        if (value.getBytes() > maxValue.getBytes()) {
            final String message = String.format(
                    Locale.ROOT,
                    "failed to parse value [%s] for setting [%s], must be <= [%s]",
                    s,
                    key,
                    maxValue.getStringRep());
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    /**
     * Creates a setting where the allowed values are defined as enum constants. All enum constants must be uppercase.
     *
     * @param clazz the enum class
     * @param key the key for the setting
     * @param defaultValue the default value for this setting
     * @param properties properties for this setting like scope, filtering...
     * @param <T> the generics type parameter reflecting the actual type of the enum
     * @return the setting object
     */
    public static <T extends Enum<T>> Setting<T> enumSetting(Class<T> clazz, String key, T defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue.toString(), e -> Enum.valueOf(clazz, e.toUpperCase(Locale.ROOT)), properties);
    }

    /**
     * Creates a setting where the allowed values are defined as enum constants. All enum constants must be uppercase.
     *
     * @param clazz the enum class
     * @param key the key for the setting
     * @param fallbackSetting the fallback setting for this setting
     * @param validator validator for this setting
     * @param properties properties for this setting like scope, filtering...
     * @param <T> the generics type parameter reflecting the actual type of the enum
     * @return the setting object
     */
    public static <T extends Enum<T>> Setting<T> enumSetting(Class<T> clazz, String key, Setting<T> fallbackSetting,
                                                             Validator<T> validator, Property... properties) {
        return new Setting<>(new SimpleKey(key), fallbackSetting, fallbackSetting::getRaw,
            e -> Enum.valueOf(clazz, e.toUpperCase(Locale.ROOT)), validator, properties);
    }

    /**
     * Creates a setting which specifies a memory size. This can either be
     * specified as an absolute bytes value or as a percentage of the heap
     * memory.
     *
     * @param key the key for the setting
     * @param defaultValue the default value for this setting
     * @param properties properties properties for this setting like scope, filtering...
     * @return the setting object
     */
    public static Setting<ByteSizeValue> memorySizeSetting(String key, ByteSizeValue defaultValue, Property... properties) {
        return memorySizeSetting(key, (s) -> defaultValue.toString(), properties);
    }


    /**
     * Creates a setting which specifies a memory size. This can either be
     * specified as an absolute bytes value or as a percentage of the heap
     * memory.
     *
     * @param key the key for the setting
     * @param defaultValue a function that supplies the default value for this setting
     * @param properties properties properties for this setting like scope, filtering...
     * @return the setting object
     */
    public static Setting<ByteSizeValue> memorySizeSetting(String key, Function<Settings, String> defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), properties);
    }

    /**
     * Creates a setting which specifies a memory size. This can either be
     * specified as an absolute bytes value or as a percentage of the heap
     * memory.
     *
     * @param key the key for the setting
     * @param defaultPercentage the default value of this setting as a percentage of the heap memory
     * @param properties properties properties for this setting like scope, filtering...
     * @return the setting object
     */
    public static Setting<ByteSizeValue> memorySizeSetting(String key, String defaultPercentage, Property... properties) {
        return new Setting<>(key, (s) -> defaultPercentage, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), properties);
    }

    public static <T> Setting<List<T>> listSetting(
            final String key,
            final List<String> defaultStringValue,
            final Function<String, T> singleValueParser,
            final Property... properties) {
        return listSetting(key, null, singleValueParser, (s) -> defaultStringValue, properties);
    }

    public static <T> Setting<List<T>> listSetting(
        final String key,
        final List<String> defaultStringValue,
        final Function<String, T> singleValueParser,
        final Validator<List<T>> validator,
        final Property... properties) {
        return listSetting(key, null, singleValueParser, (s) -> defaultStringValue, validator, properties);
    }

    // TODO this one's two argument get is still broken
    public static <T> Setting<List<T>> listSetting(
            final String key,
            final Setting<List<T>> fallbackSetting,
            final Function<String, T> singleValueParser,
            final Property... properties) {
        return listSetting(key, fallbackSetting, singleValueParser, (s) -> parseableStringToList(fallbackSetting.getRaw(s)), properties);
    }

    public static <T> Setting<List<T>> listSetting(
            final String key,
            final Function<String, T> singleValueParser,
            final Function<Settings, List<String>> defaultStringValue,
            final Property... properties) {
        return listSetting(key, null, singleValueParser, defaultStringValue, properties);
    }

    public static <T> Setting<List<T>> listSetting(
        final String key,
        final Function<String, T> singleValueParser,
        final Function<Settings, List<String>> defaultStringValue,
        final Validator<List<T>> validator,
        final Property... properties) {
        return listSetting(key, null, singleValueParser, defaultStringValue, validator, properties);
    }

    public static <T> Setting<List<T>> listSetting(
            final String key,
            final @Nullable Setting<List<T>> fallbackSetting,
            final Function<String, T> singleValueParser,
            final Function<Settings, List<String>> defaultStringValue,
            final Property... properties) {
        return listSetting(key, fallbackSetting, singleValueParser, defaultStringValue, v -> {}, properties);
    }

    public static <T> Setting<List<T>> listSetting(
        final String key,
        final @Nullable Setting<List<T>> fallbackSetting,
        final Function<String, T> singleValueParser,
        final Function<Settings, List<String>> defaultStringValue,
        final Validator<List<T>> validator,
        final Property... properties) {
        if (defaultStringValue.apply(Settings.EMPTY) == null) {
            throw new IllegalArgumentException("default value function must not return null");
        }
        Function<String, List<T>> parser = (s) ->
            parseableStringToList(s).stream().map(singleValueParser).collect(Collectors.toList());

        return new ListSetting<>(key, fallbackSetting, defaultStringValue, parser, validator, properties);
    }

    private static List<String> parseableStringToList(String parsableString) {
        // fromXContent doesn't use named xcontent or deprecation.
        try (XContentParser xContentParser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, parsableString)) {
            xContentParser.nextToken();
            return XContentParserUtils.parseList(xContentParser, p -> {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, p.currentToken(), p);
                return p.text();
            });
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse array", e);
        }
    }

    private static String arrayToParsableString(List<String> array) {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startArray();
            for (String element : array) {
                builder.value(element);
            }
            builder.endArray();
            return Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
    }

    private static class ListSetting<T> extends Setting<List<T>> {

        private final Function<Settings, List<String>> defaultStringValue;

        private ListSetting(
                final String key,
                final @Nullable Setting<List<T>> fallbackSetting,
                final Function<Settings, List<String>> defaultStringValue,
                final Function<String, List<T>> parser,
                final Validator<List<T>> validator,
                final Property... properties) {
            super(
                    new ListKey(key),
                    fallbackSetting,
                    s -> Setting.arrayToParsableString(defaultStringValue.apply(s)),
                    parser,
                    validator,
                    properties);
            this.defaultStringValue = defaultStringValue;
        }

        @Override
        String innerGetRaw(final Settings settings) {
            List<String> array = settings.getAsList(getKey(), null);
            return array == null ? defaultValue.apply(settings) : arrayToParsableString(array);
        }

        @Override
        boolean hasComplexMatcher() {
            return true;
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            if (exists(source) == false) {
                List<String> asList = defaultSettings.getAsList(getKey(), null);
                if (asList == null) {
                    builder.putList(getKey(), defaultStringValue.apply(defaultSettings));
                } else {
                    builder.putList(getKey(), asList);
                }
            }
        }
    }

    static void logSettingUpdate(Setting<?> setting, Settings current, Settings previous, Logger logger) {
        if (logger.isInfoEnabled()) {
            if (setting.isFiltered()) {
                logger.info("updating [{}]", setting.key);
            } else {
                logger.info("updating [{}] from [{}] to [{}]", setting.key, setting.getRaw(previous), setting.getRaw(current));
            }
        }
    }

    public static Setting<Settings> groupSetting(String key, Property... properties) {
        return groupSetting(key, (s) -> {}, properties);
    }

    public static Setting<Settings> groupSetting(String key, Consumer<Settings> validator, Property... properties) {
        return new GroupSetting(key, validator, properties);
    }

    public static Setting<TimeValue> timeSetting(
            final String key,
            final Setting<TimeValue> fallbackSetting,
            final TimeValue minValue,
            final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(
                simpleKey,
                fallbackSetting,
                fallbackSetting::getRaw,
                minTimeValueParser(key, minValue, isFiltered(properties)),
                v -> {},
                properties);
    }

    public static Setting<TimeValue> timeSetting(
            final String key, Function<Settings, TimeValue> defaultValue, final TimeValue minValue, final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(simpleKey, s -> defaultValue.apply(s).getStringRep(),
            minTimeValueParser(key, minValue, isFiltered(properties)), properties);
    }

    public static Setting<TimeValue> timeSetting(
          final String key, TimeValue defaultValue, final TimeValue minValue, final TimeValue maxValue, final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(simpleKey, s -> defaultValue.getStringRep(),
            minMaxTimeValueParser(key, minValue, maxValue, isFiltered(properties)), properties);
    }

    private static Function<String, TimeValue> minTimeValueParser(final String key, final TimeValue minValue, boolean isFiltered) {
        return s -> {
            TimeValue value;
            try {
                value = TimeValue.parseTimeValue(s, null, key);
            } catch (RuntimeException ex) {
                if (isFiltered) {
                    throw new IllegalArgumentException("failed to parse value for setting [" + key + "] as a time value");
                } else {
                    throw ex;
                }
            }
            if (value.millis() < minValue.millis()) {
                final String message = String.format(
                        Locale.ROOT,
                        "failed to parse value%s for setting [%s], must be >= [%s]",
                        isFiltered ? "" : " [" + s + "]",
                        key,
                        minValue.getStringRep());
                throw new IllegalArgumentException(message);
            }
            return value;
        };
    }

    private static Function<String, TimeValue> minMaxTimeValueParser(
            final String key, final TimeValue minValue, final TimeValue maxValue, boolean isFiltered) {
        return s -> {
            TimeValue value;
            try {
                value = minTimeValueParser(key, minValue, isFiltered).apply(s);
            } catch (RuntimeException ex) {
                if (isFiltered) {
                    throw new IllegalArgumentException("failed to parse value for setting [" + key + "] as a time value");
                } else {
                    throw ex;
                }
            }
            if (value.millis() > maxValue.millis()) {
                final String message = String.format(
                        Locale.ROOT,
                        "failed to parse value%s for setting [%s], must be <= [%s]",
                        isFiltered ? "" : " [" + s + "]",
                        key,
                        maxValue.getStringRep());
                throw new IllegalArgumentException(message);
            }
            return value;
        };
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, TimeValue minValue, Property... properties) {
        return timeSetting(key, (s) -> defaultValue, minValue, properties);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> defaultValue.getStringRep(), (s) -> TimeValue.parseTimeValue(s, key), properties);
    }

    public static Setting<TimeValue> timeSetting(String key, Setting<TimeValue> fallbackSetting, Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> TimeValue.parseTimeValue(s, key), properties);
    }

    public static Setting<TimeValue> timeSetting(String key, Setting<TimeValue> fallBackSetting, Validator<TimeValue> validator,
                                                 Property... properties) {
        return new Setting<>(new SimpleKey(key), fallBackSetting, fallBackSetting::getRaw, (s) -> TimeValue.parseTimeValue(s, key),
            validator, properties);
    }

    public static Setting<TimeValue> positiveTimeSetting(String key, TimeValue defaultValue, Property... properties) {
        return timeSetting(key, defaultValue, TimeValue.timeValueMillis(0), properties);
    }

    public static Setting<TimeValue> positiveTimeSetting(
            final String key,
            final Setting<TimeValue> fallbackSetting,
            final TimeValue minValue,
            final Property... properties) {
        return timeSetting(key, fallbackSetting, minValue, properties);
    }

    public static Setting<Double> doubleSetting(String key, double defaultValue, double minValue, Property... properties) {
        return doubleSetting(key, defaultValue, minValue, Double.POSITIVE_INFINITY, properties);
    }

    public static Setting<Double> doubleSetting(String key, double defaultValue, double minValue, double maxValue, Property... properties) {
        return new Setting<>(key, (s) -> Double.toString(defaultValue), (s) -> {
            final double d = Double.parseDouble(s);
            if (d < minValue) {
                String err = "Failed to parse value" + (isFiltered(properties) ? "" : " [" + s + "]") + " for setting [" + key +
                    "] must be >= " + minValue;
                throw new IllegalArgumentException(err);
            }
            if (d > maxValue) {
                String err = "Failed to parse value" + (isFiltered(properties) ? "" : " [" + s + "]") + " for setting [" + key +
                    "] must be <= " + maxValue;
                throw new IllegalArgumentException(err);
            }
            return d;
        }, properties);
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
    public static <T> AffixSetting<T> prefixKeySetting(String prefix, Function<String, Setting<T>> delegateFactory) {
        BiFunction<String, String, Setting<T>> delegateFactoryWithNamespace = (ns, k) -> delegateFactory.apply(k);
        return affixKeySetting(new AffixKey(prefix), delegateFactoryWithNamespace);
    }

    /**
     * This setting type allows to validate settings that have the same type and a common prefix and suffix. For instance
     * storage.${backend}.enable=[true|false] can easily be added with this setting. Yet, affix key settings don't support updaters
     * out of the box unless {@link #getConcreteSetting(String)} is used to pull the updater.
     */
    public static <T> AffixSetting<T> affixKeySetting(String prefix, String suffix, Function<String, Setting<T>> delegateFactory,
                                                      AffixSettingDependency... dependencies) {
        BiFunction<String, String, Setting<T>> delegateFactoryWithNamespace = (ns, k) -> delegateFactory.apply(k);
        return affixKeySetting(new AffixKey(prefix, suffix), delegateFactoryWithNamespace, dependencies);
    }

    public static <T> AffixSetting<T> affixKeySetting(String prefix, String suffix, BiFunction<String, String, Setting<T>> delegateFactory,
                                                      AffixSettingDependency... dependencies) {
        Setting<T> delegate = delegateFactory.apply("_na_", "_na_");
        return new AffixSetting<>(new AffixKey(prefix, suffix), delegate, delegateFactory, dependencies);
    }

    private static <T> AffixSetting<T> affixKeySetting(AffixKey key, BiFunction<String, String, Setting<T>> delegateFactory,
                                                       AffixSettingDependency... dependencies) {
        Setting<T> delegate = delegateFactory.apply("_na_", "_na_");
        return new AffixSetting<>(key, delegate, delegateFactory, dependencies);
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

    /**
     * A key that allows for static pre and suffix. This is used for settings
     * that have dynamic namespaces like for different accounts etc.
     */
    public static final class AffixKey implements Key {
        private final Pattern pattern;
        private final String prefix;
        private final String suffix;

        AffixKey(String prefix) {
            this(prefix, null);
        }

        AffixKey(String prefix, String suffix) {
            assert prefix != null || suffix != null: "Either prefix or suffix must be non-null";

            this.prefix = prefix;
            if (prefix.endsWith(".") == false) {
                throw new IllegalArgumentException("prefix must end with a '.'");
            }
            this.suffix = suffix;
            if (suffix == null) {
                pattern = Pattern.compile("(" + Pattern.quote(prefix) + "((?:[-\\w]+[.])*[-\\w]+$))");
            } else {
                // the last part of this regexp is to support both list and group keys
                pattern = Pattern.compile("(" + Pattern.quote(prefix) + "([-\\w]+)\\." + Pattern.quote(suffix) + ")(?:\\..*)?");
            }
        }

        @Override
        public boolean match(String key) {
            return pattern.matcher(key).matches();
        }

        /**
         * Returns a string representation of the concrete setting key
         */
        String getConcreteString(String key) {
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches() == false) {
                throw new IllegalStateException("can't get concrete string for key " + key + " key doesn't match");
            }
            return matcher.group(1);
        }

        /**
         * Returns a string representation of the concrete setting key
         */
        String getNamespace(String key) {
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches() == false) {
                throw new IllegalStateException("can't get concrete string for key " + key + " key doesn't match");
            }
            return matcher.group(2);
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
                sb.append('*');
                sb.append('.');
                sb.append(suffix);
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
