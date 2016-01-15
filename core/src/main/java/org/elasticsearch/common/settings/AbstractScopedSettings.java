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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A basic setting service that can be used for per-index and per-cluster settings.
 * This service offers transactional application of updates settings.
 */
public abstract class AbstractScopedSettings extends AbstractComponent {
    private Settings lastSettingsApplied = Settings.EMPTY;
    private final List<SettingUpdater> settingUpdaters = new ArrayList<>();
    private final Map<String, Setting<?>> complexMatchers = new HashMap<>();
    private final Map<String, Setting<?>> keySettings = new HashMap<>();
    private final Setting.Scope scope;

    protected AbstractScopedSettings(Settings settings, Set<Setting<?>> settingsSet, Setting.Scope scope) {
        super(settings);
        this.lastSettingsApplied = Settings.EMPTY;
        this.scope = scope;
        for (Setting<?> entry : settingsSet) {
            addSetting(entry);
        }
    }

    protected AbstractScopedSettings(Settings nodeSettings, Settings scopeSettings, AbstractScopedSettings other) {
        super(nodeSettings);
        this.lastSettingsApplied = scopeSettings;
        this.scope = other.scope;
        complexMatchers.putAll(other.complexMatchers);
        keySettings.putAll(other.keySettings);
        settingUpdaters.addAll(other.settingUpdaters);
    }

    protected final void addSetting(Setting<?> setting) {
        if (setting.getScope() != scope) {
            throw new IllegalArgumentException("Setting must be a " + scope + " setting but was: " + setting.getScope());
        }
        if (setting.hasComplexMatcher()) {
            complexMatchers.putIfAbsent(setting.getKey(), setting);
        } else {
            keySettings.putIfAbsent(setting.getKey(), setting);
        }
    }

    public Setting.Scope getScope() {
        return this.scope;
    }

    /**
     * Applies the given settings to all listeners and rolls back the result after application. This
     * method will not change any settings but will fail if any of the settings can't be applied.
     */
    public synchronized Settings dryRun(Settings settings) {
        final Settings current = Settings.builder().put(this.settings).put(settings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        List<RuntimeException> exceptions = new ArrayList<>();
        for (SettingUpdater settingUpdater : settingUpdaters) {
            try {
                if (settingUpdater.hasChanged(current, previous)) {
                    settingUpdater.getValue(current, previous);
                }
            } catch (RuntimeException ex) {
                exceptions.add(ex);
                logger.debug("failed to prepareCommit settings for [{}]", ex, settingUpdater);
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
            for (SettingUpdater settingUpdater : settingUpdaters) {
                try {
                    applyRunnables.add(settingUpdater.updater(current, previous));
                } catch (Exception ex) {
                    logger.warn("failed to prepareCommit settings for [{}]", ex, settingUpdater);
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
        this.settingUpdaters.add(setting.newUpdater(consumer, logger, validator));
    }

    /**
     * Adds a settings consumer that accepts the values for two settings. The consumer if only notified if one or both settings change.
     * <p>
     * Note: Only settings registered in {@link SettingsModule} can be changed dynamically.
     * </p>
     * This method registers a compound updater that is useful if two settings are depending on each other. The consumer is always provided
     * with both values even if only one of the two changes.
     */
    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        if (a != get(a.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + a.getKey() + "]");
        }
        if (b != get(b.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + b.getKey() + "]");
        }
        this.settingUpdaters.add(Setting.compoundUpdater(consumer, a, b, logger));
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
     * Validates that the setting is valid
     */
    public final void validate(String key, String value) {
        Settings.Builder builder = Settings.builder().put(key, value);
        Setting setting = get(key);
        if (setting == null) {
            throw new IllegalArgumentException("unknown setting [" + key + "]");
        }
        setting.get(builder.build());
    }

    /**
     * Transactional interface to update settings.
     * @see Setting
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
    public Setting get(String key) {
        Setting<?> setting = keySettings.get(key);
        if (setting == null) {
            for (Map.Entry<String, Setting<?>> entry : complexMatchers.entrySet()) {
                if (entry.getValue().match(key)) {
                    return entry.getValue();
                }
            }
        } else {
            return setting;
        }
        return null;
    }

    /**
     * Returns <code>true</code> if the setting for the given key is dynamically updateable. Otherwise <code>false</code>.
     */
    public boolean hasDynamicSetting(String key) {
        final Setting setting = get(key);
        return setting != null && setting.isDynamic();
    }

    /**
     * Returns a settings object that contains all settings that are not
     * already set in the given source. The diff contains either the default value for each
     * setting or the settings value in the given default settings.
     */
    public Settings diff(Settings source, Settings defaultSettings) {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> setting : keySettings.values()) {
            if (setting.exists(source) == false) {
                builder.put(setting.getKey(), setting.getRaw(defaultSettings));
            }
        }
        return builder.build();
    }

    /**
     * Returns the value for the given setting.
     */
    public <T> T get(Setting<T> setting) {
        if (setting.getScope() != scope) {
            throw new IllegalArgumentException("settings scope doesn't match the setting scope [" + this.scope + "] != [" + setting.getScope() + "]");
        }
        if (get(setting.getKey()) == null) {
            throw new IllegalArgumentException("setting " + setting.getKey() + " has not been registered");
        }
        return setting.get(this.lastSettingsApplied);
    }

    public boolean applyDynamicSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        boolean changed = false;
        final Set<String> toRemove = new HashSet<>();
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        for (Map.Entry<String, String> entry : toApply.getAsMap().entrySet()) {
            if (entry.getValue() == null) {
                toRemove.add(entry.getKey());
            } else if (hasDynamicSetting(entry.getKey())) {
                validate(entry.getKey(), entry.getValue());
                settingsBuilder.put(entry.getKey(), entry.getValue());
                updates.put(entry.getKey(), entry.getValue());
                changed = true;
            } else {
                throw new IllegalArgumentException(type + " setting [" + entry.getKey() + "], not dynamically updateable");
            }

        }
        changed |= applyDeletes(toRemove, target);
        target.put(settingsBuilder.build());
        return changed;
    }

    private static final boolean applyDeletes(Set<String> deletes, Settings.Builder builder) {
        boolean changed = false;
        for (String entry : deletes) {
            Set<String> keysToRemove = new HashSet<>();
            Set<String> keySet = builder.internalMap().keySet();
            for (String key : keySet) {
                if (Regex.simpleMatch(entry, key)) {
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

}
