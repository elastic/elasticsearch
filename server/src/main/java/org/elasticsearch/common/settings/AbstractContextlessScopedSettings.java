/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.TriConsumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractContextlessScopedSettings extends AbstractScopedSettings<Void> {

    protected final Settings settings;
    private Settings lastSettingsApplied;

    public AbstractContextlessScopedSettings(
        Settings nodeSettings,
        Settings scopeSettings,
        AbstractContextlessScopedSettings other,
        Logger logger
    ) {
        super(other, logger);

        this.settings = nodeSettings;
        this.lastSettingsApplied = scopeSettings;
    }

    public AbstractContextlessScopedSettings(Settings settings, Set<Setting<?>> settingsSet, Setting.Property scope) {
        super(settingsSet, scope);

        this.settings = settings;
        this.lastSettingsApplied = Settings.EMPTY;
    }

    /**
     * Validates the given settings by running it through all update listeners without applying it. This
     * method will not change any settings but will fail if any of the settings can't be applied.
     */
    public synchronized Settings validateUpdate(Settings settings) {
        final Settings current = Settings.builder().put(this.settings).put(settings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        validateUpdate(current, previous);

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
        executeSettingsUpdaters(null, current, previous);

        return lastSettingsApplied = newSettings;
    }

    /**
     * Returns the value for the given setting.
     */
    public <T> T get(Setting<T> setting) {
        if (setting.getProperties().contains(scope) == false) {
            throw new IllegalArgumentException(
                "settings scope doesn't match the setting scope [" + this.scope + "] not in [" + setting.getProperties() + "]"
            );
        }
        if (get(setting.getKey()) == null) {
            throw new IllegalArgumentException("setting " + setting.getKey() + " has not been registered");
        }
        return setting.get(this.lastSettingsApplied, settings);
    }

    private static <T, V> TriConsumer<Void, T, V> wrapIgnoringContext(BiConsumer<T, V> consumer) {
        return (ctx, t, v) -> consumer.accept(t, v);
    }

    private static <V> BiConsumer<Void, V> wrapIgnoringContext(Consumer<V> consumer) {
        return (ctx, v) -> consumer.accept(v);
    }

    /**
     * Adds a settings consumer with a predicate that is only evaluated at update time.
     * <p>
     * Note: Only settings registered in {@link SettingsModule} can be changed dynamically.
     * </p>
     * @param <T>       The type of the setting's value.
     * @param setting   The setting for which the updates are to be handled.
     * @param consumer  A {@link BiConsumer} that will be executed with the updated setting value.
     * @param validator an additional validator that is only applied to updates of this setting.
     *                  This is useful to add additional validation to settings at runtime compared to at startup time.
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        super.addSettingsUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
    }

    /**
     * Adds a settings consumer.
     * <p>
     * Note: Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     * </p>
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        super.addSettingsUpdateConsumer(setting, wrapIgnoringContext(consumer));
    }

    /**
     * Adds a settings consumer that is only executed if any setting in the supplied list of settings is changed. In that case all the
     * settings are specified in the argument are returned.
     *
     * Also automatically adds empty consumers for all settings in order to activate logging
     */
    public synchronized void addSettingsUpdateConsumer(Consumer<Settings> consumer, List<? extends Setting<?>> settings) {
        super.addSettingsUpdateConsumer(wrapIgnoringContext(consumer), settings);
    }

    /**
     * Adds a settings consumer that is only executed if any setting in the supplied list of settings is changed. In that case all the
     * settings are specified in the argument are returned.  The validator is run across all specified settings before the settings are
     * applied.
     *
     * Also automatically adds empty consumers for all settings in order to activate logging
     */
    public synchronized void addSettingsUpdateConsumer(
        Consumer<Settings> consumer,
        List<? extends Setting<?>> settings,
        Consumer<Settings> validator
    ) {
        super.addSettingsUpdateConsumer(wrapIgnoringContext(consumer), settings, validator);
    }

    /**
     * Adds a settings consumer for affix settings. Affix settings have a namespace associated to it that needs to be available to the
     * consumer in order to be processed correctly.
     */
    public synchronized <T> void addAffixUpdateConsumer(
        Setting.AffixSetting<T> setting,
        BiConsumer<String, T> consumer,
        BiConsumer<String, T> validator
    ) {
        super.addAffixUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
    }

    /**
     * Adds a affix settings consumer that accepts the settings for a group of settings. The consumer is only
     * notified if at least one of the settings change.
     * <p>
     * Note: Only settings registered in {@link SettingsModule} can be changed dynamically.
     * </p>
     */
    public synchronized void addAffixGroupUpdateConsumer(List<Setting.AffixSetting<?>> settings, BiConsumer<String, Settings> consumer) {
        super.addAffixGroupUpdateConsumer(settings, wrapIgnoringContext(consumer));
    }

    /**
     * Adds a settings consumer for affix settings. Affix settings have a namespace associated to it that needs to be available to the
     * consumer in order to be processed correctly. This consumer will get a namespace to value map instead of each individual namespace
     * and value as in {@link #addAffixUpdateConsumer(Setting.AffixSetting, BiConsumer, BiConsumer)}
     */
    public synchronized <T> void addAffixMapUpdateConsumer(
        Setting.AffixSetting<T> setting,
        Consumer<Map<String, T>> consumer,
        BiConsumer<String, T> validator
    ) {
        super.addAffixMapUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
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
    public synchronized <A, B> void addSettingsUpdateConsumer(
        Setting<A> a,
        Setting<B> b,
        BiConsumer<A, B> consumer,
        BiConsumer<A, B> validator
    ) {
        super.addSettingsUpdateConsumer(a, b, wrapIgnoringContext(consumer), validator);
    }

    /**
     * Adds a settings consumer that accepts the values for two settings.
     * See {@link #addSettingsUpdateConsumer(Setting, Setting, BiConsumer, BiConsumer)} for details.
     */
    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        super.addSettingsUpdateConsumer(a, b, wrapIgnoringContext(consumer));
    }
}
