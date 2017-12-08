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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A basic setting service that can be used for per-index and per-cluster settings.
 * This service offers transactional application of updates settings.
 */
public abstract class AbstractScopedSettings extends AbstractComponent {
    public static final String ARCHIVED_SETTINGS_PREFIX = "archived.";
    private Settings lastSettingsApplied = Settings.EMPTY;
    private final List<SettingUpdater<?>> settingUpdaters = new CopyOnWriteArrayList<>();
    private final Map<String, Setting<?>> complexMatchers;
    private final Map<String, Setting<?>> keySettings;
    private final Setting.Property scope;
    private static final Pattern KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])*[-\\w]+$");
    private static final Pattern GROUP_KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])+$");
    private static final Pattern AFFIX_KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])+[*](?:[.][-\\w]+)+$");

    protected AbstractScopedSettings(Settings settings, Set<Setting<?>> settingsSet, Setting.Property scope) {
        super(settings);
        this.lastSettingsApplied = Settings.EMPTY;
        this.scope = scope;
        Map<String, Setting<?>> complexMatchers = new HashMap<>();
        Map<String, Setting<?>> keySettings = new HashMap<>();
        for (Setting<?> setting : settingsSet) {
            if (setting.getProperties().contains(scope) == false) {
                throw new IllegalArgumentException("Setting must be a " + scope + " setting but has: " + setting.getProperties());
            }
            validateSettingKey(setting);

            if (setting.hasComplexMatcher()) {
                Setting<?> overlappingSetting = findOverlappingSetting(setting, complexMatchers);
                if (overlappingSetting != null) {
                    throw new IllegalArgumentException("complex setting key: [" + setting.getKey() + "] overlaps existing setting key: [" +
                        overlappingSetting.getKey() + "]");
                }
                complexMatchers.putIfAbsent(setting.getKey(), setting);
            } else {
                keySettings.putIfAbsent(setting.getKey(), setting);
            }
        }
        this.complexMatchers = Collections.unmodifiableMap(complexMatchers);
        this.keySettings = Collections.unmodifiableMap(keySettings);
    }

    protected void validateSettingKey(Setting setting) {
        if (isValidKey(setting.getKey()) == false && (setting.isGroupSetting() && isValidGroupKey(setting.getKey())
            || isValidAffixKey(setting.getKey())) == false || setting.getKey().endsWith(".0")) {
            throw new IllegalArgumentException("illegal settings key: [" + setting.getKey() + "]");
        }
    }

    protected AbstractScopedSettings(Settings nodeSettings, Settings scopeSettings, AbstractScopedSettings other) {
        super(nodeSettings);
        this.lastSettingsApplied = scopeSettings;
        this.scope = other.scope;
        complexMatchers = other.complexMatchers;
        keySettings = other.keySettings;
        settingUpdaters.addAll(other.settingUpdaters);
    }

    /**
     * Returns <code>true</code> iff the given key is a valid settings key otherwise <code>false</code>
     */
    public static boolean isValidKey(String key) {
        return KEY_PATTERN.matcher(key).matches();
    }

    private static boolean isValidGroupKey(String key) {
        return GROUP_KEY_PATTERN.matcher(key).matches();
    }

    // pkg private for tests
    static boolean isValidAffixKey(String key) {
        return AFFIX_KEY_PATTERN.matcher(key).matches();
    }

    public Setting.Property getScope() {
        return this.scope;
    }

    /**
     * Validates the given settings by running it through all update listeners without applying it. This
     * method will not change any settings but will fail if any of the settings can't be applied.
     */
    public synchronized Settings validateUpdate(Settings settings) {
        final Settings current = Settings.builder().put(this.settings).put(settings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        List<RuntimeException> exceptions = new ArrayList<>();
        for (SettingUpdater<?> settingUpdater : settingUpdaters) {
            try {
                // ensure running this through the updater / dynamic validator
                // don't check if the value has changed we wanna test this anyways
                settingUpdater.getValue(current, previous);
            } catch (RuntimeException ex) {
                exceptions.add(ex);
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to prepareCommit settings for [{}]", settingUpdater), ex);
            }
        }
        // here we are exhaustive and record all settings that failed.
        ExceptionsHelper.rethrowAndSuppress(exceptions);
        return current;
    }

    /**
     * Applies the given settings to all the settings consumers or to none of them. The settings
     * will be merged with the node settings before they are applied while given settings override existing node
     * settings.
     * @param newSettings the settings to apply
     * @return the unmerged applied settings
    */
    public synchronized Settings applySettings(Settings newSettings) {
        if (lastSettingsApplied != null && newSettings.equals(lastSettingsApplied)) {
            // nothing changed in the settings, ignore
            return newSettings;
        }
        final Settings current = Settings.builder().put(this.settings).put(newSettings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        try {
            List<Runnable> applyRunnables = new ArrayList<>();
            for (SettingUpdater<?> settingUpdater : settingUpdaters) {
                try {
                    applyRunnables.add(settingUpdater.updater(current, previous));
                } catch (Exception ex) {
                    logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage("failed to prepareCommit settings for [{}]", settingUpdater), ex);
                    throw ex;
                }
            }
            for (Runnable settingUpdater : applyRunnables) {
                settingUpdater.run();
            }
        } catch (Exception ex) {
            logger.warn("failed to apply settings", ex);
            throw ex;
        } finally {
        }
        return lastSettingsApplied = newSettings;
    }

    /**
     * Adds a settings consumer with a predicate that is only evaluated at update time.
     * <p>
     * Note: Only settings registered in {@link SettingsModule} can be changed dynamically.
     * </p>
     * @param validator an additional validator that is only applied to updates of this setting.
     *                  This is useful to add additional validation to settings at runtime compared to at startup time.
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        if (setting != get(setting.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        addSettingsUpdater(setting.newUpdater(consumer, logger, validator));
    }

    /**
     * Adds a settings consumer for affix settings. Affix settings have a namespace associated to it that needs to be available to the
     * consumer in order to be processed correctly.
     */
    public synchronized <T> void addAffixUpdateConsumer(Setting.AffixSetting<T> setting,  BiConsumer<String, T> consumer,
                                                        BiConsumer<String, T> validator) {
        final Setting<?> registeredSetting = this.complexMatchers.get(setting.getKey());
        if (setting != registeredSetting) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        addSettingsUpdater(setting.newAffixUpdater(consumer, logger, validator));
    }

    /**
     * Adds a settings consumer for affix settings. Affix settings have a namespace associated to it that needs to be available to the
     * consumer in order to be processed correctly. This consumer will get a namespace to value map instead of each individual namespace
     * and value as in {@link #addAffixUpdateConsumer(Setting.AffixSetting, BiConsumer, BiConsumer)}
     */
    public synchronized <T> void addAffixMapUpdateConsumer(Setting.AffixSetting<T> setting,  Consumer<Map<String, T>> consumer,
                                                        BiConsumer<String, T> validator, boolean omitDefaults) {
        final Setting<?> registeredSetting = this.complexMatchers.get(setting.getKey());
        if (setting != registeredSetting) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        addSettingsUpdater(setting.newAffixMapUpdater(consumer, logger, validator, omitDefaults));
    }

    synchronized void addSettingsUpdater(SettingUpdater<?> updater) {
        this.settingUpdaters.add(updater);
    }

    /**
     * Adds a settings consumer that accepts the values for two settings.
     * See {@link #addSettingsUpdateConsumer(Setting, Setting, BiConsumer, BiConsumer)} for details.
     */
    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        addSettingsUpdateConsumer(a, b, consumer, (i, j) -> {} );
    }

    /**
     * Adds a settings consumer that accepts the values for two settings. The consumer is only notified if one or both settings change
     * and if the provided validator succeeded.
     * <p>
     * Note: Only settings registered in {@link SettingsModule} can be changed dynamically.
     * </p>
     * This method registers a compound updater that is useful if two settings are depending on each other.
     * The consumer is always provided with both values even if only one of the two changes.
     */
    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b,
                                                              BiConsumer<A, B> consumer, BiConsumer<A, B> validator) {
        if (a != get(a.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + a.getKey() + "]");
        }
        if (b != get(b.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + b.getKey() + "]");
        }
        addSettingsUpdater(Setting.compoundUpdater(consumer, validator, a, b, logger));
    }

    /**
     * Adds a settings consumer.
     * <p>
     * Note: Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     * </p>
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
       addSettingsUpdateConsumer(setting, consumer, (s) -> {});
    }

    /**
     * Validates that all given settings are registered and valid
     * @param settings the settings to validate
     * @param validateDependencies if <code>true</code> settings dependencies are validated as well.
     * @see Setting#getSettingsDependencies(String)
     */
    public final void validate(Settings settings, boolean validateDependencies) {
        List<RuntimeException> exceptions = new ArrayList<>();
        for (String key : settings.keySet()) { // settings iterate in deterministic fashion
            try {
                validate(key, settings, validateDependencies);
            } catch (RuntimeException ex) {
                exceptions.add(ex);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    /**
     * Validates that the setting is valid
     */
    void validate(String key, Settings settings, boolean validateDependencies) {
        Setting setting = getRaw(key);
        if (setting == null) {
            LevensteinDistance ld = new LevensteinDistance();
            List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
            for (String k : this.keySettings.keySet()) {
                float distance = ld.getDistance(key, k);
                if (distance > 0.7f) {
                    scoredKeys.add(new Tuple<>(distance, k));
                }
            }
            CollectionUtil.timSort(scoredKeys, (a,b) -> b.v1().compareTo(a.v1()));
            String msgPrefix = "unknown setting";
            SecureSettings secureSettings = settings.getSecureSettings();
            if (secureSettings != null && settings.getSecureSettings().getSettingNames().contains(key)) {
                msgPrefix = "unknown secure setting";
            }
            String msg = msgPrefix + " [" + key + "]";
            List<String> keys = scoredKeys.stream().map((a) -> a.v2()).collect(Collectors.toList());
            if (keys.isEmpty() == false) {
                msg += " did you mean " + (keys.size() == 1 ? "[" + keys.get(0) + "]": "any of " + keys.toString()) + "?";
            } else {
                msg += " please check that any required plugins are installed, or check the breaking changes documentation for removed " +
                    "settings";
            }
            throw new IllegalArgumentException(msg);
        } else  {
            Set<String> settingsDependencies = setting.getSettingsDependencies(key);
            if (setting.hasComplexMatcher()) {
                setting = setting.getConcreteSetting(key);
            }
            if (validateDependencies && settingsDependencies.isEmpty() == false) {
                Set<String> settingKeys = settings.keySet();
                for (String requiredSetting : settingsDependencies) {
                    if (settingKeys.contains(requiredSetting) == false) {
                        throw new IllegalArgumentException("Missing required setting ["
                            + requiredSetting + "] for setting [" + setting.getKey() + "]");
                    }
                }
            }
        }
        setting.get(settings);
    }

    /**
     * Transactional interface to update settings.
     * @see Setting
     * @param <T> the type of the value of the setting
     */
    public interface SettingUpdater<T> {

        /**
         * Returns true if this updaters setting has changed with the current update
         * @param current the current settings
         * @param previous the previous setting
         * @return true if this updaters setting has changed with the current update
         */
        boolean hasChanged(Settings current, Settings previous);

        /**
         * Returns the instance value for the current settings. This method is stateless and idempotent.
         * This method will throw an exception if the source of this value is invalid.
         */
        T getValue(Settings current, Settings previous);

        /**
         * Applies the given value to the updater. This methods will actually run the update.
         */
        void apply(T value, Settings current, Settings previous);

        /**
         * Updates this updaters value if it has changed.
         * @return <code>true</code> iff the value has been updated.
         */
        default boolean apply(Settings current, Settings previous) {
            if (hasChanged(current, previous)) {
                T value = getValue(current, previous);
                apply(value, current, previous);
                return true;
            }
            return false;
        }

        /**
         * Returns a callable runnable that calls {@link #apply(Object, Settings, Settings)} if the settings
         * actually changed. This allows to defer the update to a later point in time while keeping type safety.
         * If the value didn't change the returned runnable is a noop.
         */
        default Runnable updater(Settings current, Settings previous) {
            if (hasChanged(current, previous)) {
                T value = getValue(current, previous);
                return () -> { apply(value, current, previous);};
            }
            return () -> {};
        }
    }

    /**
     * Returns the {@link Setting} for the given key or <code>null</code> if the setting can not be found.
     */
    public final Setting<?> get(String key) {
        Setting<?> raw = getRaw(key);
        if (raw == null) {
            return null;
        } if (raw.hasComplexMatcher()) {
            return raw.getConcreteSetting(key);
        } else {
            return raw;
        }
    }

    private Setting<?> getRaw(String key) {
        Setting<?> setting = keySettings.get(key);
        if (setting != null) {
            return setting;
        }
        for (Map.Entry<String, Setting<?>> entry : complexMatchers.entrySet()) {
            if (entry.getValue().match(key)) {
                assert assertMatcher(key, 1);
                assert entry.getValue().hasComplexMatcher();
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean assertMatcher(String key, int numComplexMatchers) {
        List<Setting<?>> list = new ArrayList<>();
        for (Map.Entry<String, Setting<?>> entry : complexMatchers.entrySet()) {
            if (entry.getValue().match(key)) {
                list.add(entry.getValue().getConcreteSetting(key));
            }
        }
        assert list.size() == numComplexMatchers : "Expected " + numComplexMatchers + " complex matchers to match key [" +
            key + "] but got: "  + list.toString();
        return true;
    }

    /**
     * Returns <code>true</code> if the setting for the given key is dynamically updateable. Otherwise <code>false</code>.
     */
    public boolean isDynamicSetting(String key) {
        final Setting<?> setting = get(key);
        return setting != null && setting.isDynamic();
    }

    /**
     * Returns <code>true</code> if the setting for the given key is final. Otherwise <code>false</code>.
     */
    public boolean isFinalSetting(String key) {
        final Setting<?> setting = get(key);
        return setting != null && setting.isFinal();
    }

    /**
     * Returns a settings object that contains all settings that are not
     * already set in the given source. The diff contains either the default value for each
     * setting or the settings value in the given default settings.
     */
    public Settings diff(Settings source, Settings defaultSettings) {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> setting : keySettings.values()) {
            setting.diff(builder, source, defaultSettings);
        }
        for (Setting<?> setting : complexMatchers.values()) {
            setting.diff(builder, source, defaultSettings);
        }
        return builder.build();
    }

    /**
     * Returns the value for the given setting.
     */
    public <T> T get(Setting<T> setting) {
        if (setting.getProperties().contains(scope) == false) {
            throw new IllegalArgumentException("settings scope doesn't match the setting scope [" + this.scope + "] not in [" +
                setting.getProperties() + "]");
        }
        if (get(setting.getKey()) == null) {
            throw new IllegalArgumentException("setting " + setting.getKey() + " has not been registered");
        }
        return setting.get(this.lastSettingsApplied, settings);
    }

    /**
     * Updates a target settings builder with new, updated or deleted settings from a given settings builder.
     * <p>
     * Note: This method will only allow updates to dynamic settings. if a non-dynamic setting is updated an
     * {@link IllegalArgumentException} is thrown instead.
     * </p>
     *
     * @param toApply the new settings to apply
     * @param target the target settings builder that the updates are applied to. All keys that have explicit null value in toApply will be
     *        removed from this builder
     * @param updates a settings builder that holds all updates applied to target
     * @param type a free text string to allow better exceptions messages
     * @return <code>true</code> if the target has changed otherwise <code>false</code>
     */
    public boolean updateDynamicSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        return updateSettings(toApply, target, updates, type, true);
    }

    /**
     * Updates a target settings builder with new, updated or deleted settings from a given settings builder.
     *
     * @param toApply the new settings to apply
     * @param target the target settings builder that the updates are applied to. All keys that have explicit null value in toApply will be
     *        removed from this builder
     * @param updates a settings builder that holds all updates applied to target
     * @param type a free text string to allow better exceptions messages
     * @return <code>true</code> if the target has changed otherwise <code>false</code>
     */
    public boolean updateSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        return updateSettings(toApply, target, updates, type, false);
    }

    /**
     * Returns <code>true</code> if the given key is a valid delete key
     */
    private boolean isValidDelete(String key, boolean onlyDynamic) {
        return isFinalSetting(key) == false && // it's not a final setting
            (onlyDynamic && isDynamicSetting(key)  // it's a dynamicSetting and we only do dynamic settings
                || get(key) == null && key.startsWith(ARCHIVED_SETTINGS_PREFIX) // the setting is not registered AND it's been archived
                || (onlyDynamic == false && get(key) != null)); // if it's not dynamic AND we have a key
    }

    /**
     * Updates a target settings builder with new, updated or deleted settings from a given settings builder.
     *
     * @param toApply the new settings to apply
     * @param target the target settings builder that the updates are applied to. All keys that have explicit null value in toApply will be
     *        removed from this builder
     * @param updates a settings builder that holds all updates applied to target
     * @param type a free text string to allow better exceptions messages
     * @param onlyDynamic if <code>false</code> all settings are updated otherwise only dynamic settings are updated. if set to
     *        <code>true</code> and a non-dynamic setting is updated an exception is thrown.
     * @return <code>true</code> if the target has changed otherwise <code>false</code>
     */
    private boolean updateSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type, boolean onlyDynamic) {
        boolean changed = false;
        final Set<String> toRemove = new HashSet<>();
        Settings.Builder settingsBuilder = Settings.builder();
        final Predicate<String> canUpdate = (key) -> (
            isFinalSetting(key) == false && // it's not a final setting
                ((onlyDynamic == false && get(key) != null) || isDynamicSetting(key)));
        for (String key : toApply.keySet()) {
            boolean isDelete = toApply.hasValue(key) == false;
            if (isDelete && (isValidDelete(key, onlyDynamic) || key.endsWith("*"))) {
                // this either accepts null values that suffice the canUpdate test OR wildcard expressions (key ends with *)
                // we don't validate if there is any dynamic setting with that prefix yet we could do in the future
                toRemove.add(key);
                // we don't set changed here it's set after we apply deletes below if something actually changed
            } else if (get(key) == null) {
                throw new IllegalArgumentException(type + " setting [" + key + "], not recognized");
            } else if (isDelete == false && canUpdate.test(key)) {
                validate(key, toApply, false); // we might not have a full picture here do to a dependency validation
                settingsBuilder.copy(key, toApply);
                updates.copy(key, toApply);
                changed = true;
            } else {
                if (isFinalSetting(key)) {
                    throw new IllegalArgumentException("final " + type + " setting [" + key + "], not updateable");
                } else {
                    throw new IllegalArgumentException(type + " setting [" + key + "], not dynamically updateable");
                }
            }
        }
        changed |= applyDeletes(toRemove, target, k -> isValidDelete(k, onlyDynamic));
        target.put(settingsBuilder.build());
        return changed;
    }

    private static boolean applyDeletes(Set<String> deletes, Settings.Builder builder, Predicate<String> canRemove) {
        boolean changed = false;
        for (String entry : deletes) {
            Set<String> keysToRemove = new HashSet<>();
            Set<String> keySet = builder.keys();
            for (String key : keySet) {
                if (Regex.simpleMatch(entry, key) && canRemove.test(key)) {
                    // we have to re-check with canRemove here since we might have a wildcard expression foo.* that matches
                    // dynamic as well as static settings if that is the case we might remove static settings since we resolve the
                    // wildcards late
                    keysToRemove.add(key);
                }
            }
            for (String key : keysToRemove) {
                builder.remove(key);
                changed = true;
            }
        }
        return changed;
    }

    private static Setting<?> findOverlappingSetting(Setting<?> newSetting, Map<String, Setting<?>> complexMatchers) {
        assert newSetting.hasComplexMatcher();
        if (complexMatchers.containsKey(newSetting.getKey())) {
            // we return null here because we use a putIfAbsent call when inserting into the map, so if it exists then we already checked
            // the setting to make sure there are no overlapping settings.
            return null;
        }

        for (Setting<?> existingSetting : complexMatchers.values()) {
            if (newSetting.match(existingSetting.getKey()) || existingSetting.match(newSetting.getKey())) {
                return existingSetting;
            }
        }
        return null;
    }

    /**
     * Archives invalid or unknown settings. Any setting that is not recognized or fails validation
     * will be archived. This means the setting is prefixed with {@value ARCHIVED_SETTINGS_PREFIX}
     * and remains in the settings object. This can be used to detect invalid settings via APIs.
     *
     * @param settings        the {@link Settings} instance to scan for unknown or invalid settings
     * @param unknownConsumer callback on unknown settings (consumer receives unknown key and its
     *                        associated value)
     * @param invalidConsumer callback on invalid settings (consumer receives invalid key, its
     *                        associated value and an exception)
     * @return a {@link Settings} instance with the unknown or invalid settings archived
     */
    public Settings archiveUnknownOrInvalidSettings(
        final Settings settings,
        final Consumer<Map.Entry<String, String>> unknownConsumer,
        final BiConsumer<Map.Entry<String, String>, IllegalArgumentException> invalidConsumer) {
        Settings.Builder builder = Settings.builder();
        boolean changed = false;
        for (String key : settings.keySet()) {
            try {
                Setting<?> setting = get(key);
                if (setting != null) {
                    setting.get(settings);
                    builder.copy(key, settings);
                } else {
                    if (key.startsWith(ARCHIVED_SETTINGS_PREFIX) || isPrivateSetting(key)) {
                        builder.copy(key, settings);
                    } else {
                        changed = true;
                        unknownConsumer.accept(new Entry(key, settings));
                        /*
                         * We put them back in here such that tools can check from the outside if there are any indices with invalid
                         * settings. The setting can remain there but we want users to be aware that some of their setting are invalid and
                         * they can research why and what they need to do to replace them.
                         */
                        builder.copy(ARCHIVED_SETTINGS_PREFIX + key, key, settings);
                    }
                }
            } catch (IllegalArgumentException ex) {
                changed = true;
                invalidConsumer.accept(new Entry(key, settings), ex);
                /*
                 * We put them back in here such that tools can check from the outside if there are any indices with invalid settings. The
                 * setting can remain there but we want users to be aware that some of their setting are invalid and they can research why
                 * and what they need to do to replace them.
                 */
                builder.copy(ARCHIVED_SETTINGS_PREFIX + key, key, settings);
            }
        }
        if (changed) {
            return builder.build();
        } else {
            return settings;
        }
    }

    private static final class Entry implements Map.Entry<String, String> {

        private final String key;
        private final Settings settings;

        private Entry(String key, Settings settings) {
            this.key = key;
            this.settings = settings;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return settings.get(key);
        }

        @Override
        public String setValue(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns <code>true</code> iff the setting is a private setting ie. it should be treated as valid even though it has no internal
     * representation. Otherwise <code>false</code>
     */
    // TODO this should be replaced by Setting.Property.HIDDEN or something like this.
    public boolean isPrivateSetting(String key) {
        return false;
    }
}
