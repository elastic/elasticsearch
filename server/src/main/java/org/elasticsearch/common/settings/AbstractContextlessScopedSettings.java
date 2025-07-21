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

    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        super.addSettingsUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
    }

    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        super.addSettingsUpdateConsumer(setting, wrapIgnoringContext(consumer));
    }

    public synchronized void addSettingsUpdateConsumer(Consumer<Settings> consumer, List<? extends Setting<?>> settings) {
        super.addSettingsUpdateConsumer(wrapIgnoringContext(consumer), settings);
    }

    public synchronized void addSettingsUpdateConsumer(
        Consumer<Settings> consumer,
        List<? extends Setting<?>> settings,
        Consumer<Settings> validator
    ) {
        super.addSettingsUpdateConsumer(wrapIgnoringContext(consumer), settings, validator);
    }

    public synchronized <T> void addAffixUpdateConsumer(
        Setting.AffixSetting<T> setting,
        BiConsumer<String, T> consumer,
        BiConsumer<String, T> validator
    ) {
        super.addAffixUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
    }

    public synchronized void addAffixGroupUpdateConsumer(List<Setting.AffixSetting<?>> settings, BiConsumer<String, Settings> consumer) {
        super.addAffixGroupUpdateConsumer(settings, wrapIgnoringContext(consumer));
    }

    public synchronized <T> void addAffixMapUpdateConsumer(
        Setting.AffixSetting<T> setting,
        Consumer<Map<String, T>> consumer,
        BiConsumer<String, T> validator
    ) {
        super.addAffixMapUpdateConsumer(setting, wrapIgnoringContext(consumer), validator);
    }

    public synchronized <A, B> void addSettingsUpdateConsumer(
        Setting<A> a,
        Setting<B> b,
        BiConsumer<A, B> consumer,
        BiConsumer<A, B> validator
    ) {
        super.addSettingsUpdateConsumer(a, b, wrapIgnoringContext(consumer), validator);
    }

    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        super.addSettingsUpdateConsumer(a, b, wrapIgnoringContext(consumer));
    }
}
