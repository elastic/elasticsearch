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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A basic setting service that can be used for per-index and per-cluster settings.
 * This service offers transactional application of updates settings.
 */
public abstract class SettingsService extends AbstractComponent {
    private Settings lastSettingsApplied;
    private final List<SettingUpdater> settingUpdaters = new ArrayList<>();

    protected SettingsService(Settings settings) {
        super(settings);
    }

    /**
     * Applies the given settings to all listeners and rolls back the result after application. This
     * method will not change any settings but will fail if any of the settings can't be applied.
     */
    public synchronized Settings dryRun(Settings settings) {
        final Settings build = Settings.builder().put(this.settings).put(settings).build();
        try {
            List<RuntimeException> exceptions = new ArrayList<>();
            for (SettingUpdater settingUpdater : settingUpdaters) {
                try {
                    settingUpdater.prepareApply(build);
                } catch (RuntimeException ex) {
                    exceptions.add(ex);
                    logger.debug("failed to prepareCommit settings for [{}]", ex, settingUpdater);
                }
            }
            // here we are exhaustive and record all settings that failed.
            ExceptionsHelper.rethrowAndSuppress(exceptions);
        } finally {
            for (SettingUpdater settingUpdater : settingUpdaters) {
                try {
                    settingUpdater.rollback();
                } catch (Exception e) {
                    logger.warn("failed to rollback settings for [{}]", e, settingUpdater);
                }
            }
        }
        return build;
    }

    /**
     * Applies the given settings to all the settings consumers or to none of them. The settings
     * will be merged with the node settings before they are applied while given settings override existing node
     * settings.
     * @param settings the settings to apply
     * @return the unmerged applied settings
    */
    public synchronized Settings applySettings(Settings settings) {
        if (lastSettingsApplied != null && settings.equals(lastSettingsApplied)) {
            // nothing changed in the settings, ignore
            return settings;
        }
        final Settings build = Settings.builder().put(this.settings).put(settings).build();
        boolean success = false;
        try {
            for (SettingUpdater settingUpdater : settingUpdaters) {
                try {
                    settingUpdater.prepareApply(build);
                } catch (Exception ex) {
                    logger.warn("failed to prepareCommit settings for [{}]", ex, settingUpdater);
                    throw ex;
                }
            }
            for (SettingUpdater settingUpdater : settingUpdaters) {
                settingUpdater.apply();
            }
            success = true;
        } catch (Exception ex) {
            logger.warn("failed to apply settings", ex);
            throw ex;
        } finally {
            if (success == false) {
                for (SettingUpdater settingUpdater : settingUpdaters) {
                    try {
                        settingUpdater.rollback();
                    } catch (Exception e) {
                        logger.warn("failed to refresh settings for [{}]", e, settingUpdater);
                    }
                }
            }
        }

        try {
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("logger.")) {
                    String component = entry.getKey().substring("logger.".length());
                    if ("_root".equals(component)) {
                        ESLoggerFactory.getRootLogger().setLevel(entry.getValue());
                    } else {
                        ESLoggerFactory.getLogger(component).setLevel(entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("failed to refresh settings for [{}]", e, "logger");
        }

        return lastSettingsApplied = settings;
    }

    /**
     * Adds a settings consumer with a predicate that is only evaluated at update time.
     * <p>
     * Note: Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     * </p>
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Predicate<T> predicate) {
        if (setting != getSetting(setting.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        this.settingUpdaters.add(setting.newUpdater(consumer, logger, settings, predicate));
    }

    /**
     * Adds a settings consumer that accepts the values for two settings. The consumer if only notified if one or both settings change.
     * <p>
     * Note: Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     * </p>
     */
    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        if (a != getSetting(a.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + a.getKey() + "]");
        }
        if (b != getSetting(b.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + b.getKey() + "]");
        }
        this.settingUpdaters.add(Setting.compoundUpdater(consumer, a, b, logger, settings));
    }

    /**
     * Adds a settings consumer.
     * <p>
     * Note: Only settings registered in {@link org.elasticsearch.cluster.ClusterModule} can be changed dynamically.
     * </p>
     */
    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
       addSettingsUpdateConsumer(setting, consumer, (s) -> true);
    }

    protected abstract Setting<?> getSetting(String key);

    /**
     * Transactional interface to update settings.
     * @see Setting
     */
    public interface SettingUpdater {
        /**
         * Prepares applying the given settings to this updater. All the heavy lifting like parsing and validation
         * happens in this method. Yet the actual setting should not be changed by this call.
         * @param settings the settings to apply
         * @return <code>true</code> if this updater will update a setting on calling {@link #apply()} otherwise <code>false</code>
         */
        boolean prepareApply(Settings settings);

        /**
         * Applies the settings passed to {@link #prepareApply(Settings)}
         */
        void apply();

        /**
         * Rolls back to the state before {@link #prepareApply(Settings)} was called. All internal prepared state is cleared after this call.
         */
        void rollback();
    }

}
