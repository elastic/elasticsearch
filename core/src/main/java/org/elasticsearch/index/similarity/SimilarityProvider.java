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

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Provider for {@link Similarity} instances
 */
public abstract class SimilarityProvider {
    protected static final Setting<String> TYPE_SETTING = Setting.simpleString("type");

    private final String name;
    private final Map<String, Setting<?> > settingMap;
    private Settings currentSettings;

    /**
     *
     * @param name The name associated with the Provider
     * @param settings The settings for this similarity provider
     */
    public SimilarityProvider(String name, Settings settings) {
        this.name = name;
        this.settingMap = getSettings().stream().collect(Collectors.toMap(Setting::getKey, setting -> setting));
        this.settingMap.put(TYPE_SETTING.getKey(), TYPE_SETTING);
        validateSettings(settings);
        this.currentSettings = settings;
    }

    /**
     * Returns the name associated with the Provider
     *
     * @return Name of the Provider
     */
    public final String name() {
        return name;
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this Provider.
     */
    public List<Setting<?>> getSettings() { return Collections.emptyList(); }


    /**
     * Returns the {@link Similarity} the Provider is for
     *
     * This method is always invoked from a synchronized context
     * @return Provided {@link Similarity}
     */
    protected abstract Similarity doGet();

    public final synchronized Similarity get() {
        return doGet();
    }

    /**
     * Called before {@link SimilarityProvider#doUpdateSettings(Settings)}
     *
     * @param newSettings The settings to validate for this similarity provider
     */
    protected void doValidateUpdateSettings(Settings newSettings) {}

    /**
     * Called when the {@link Settings} are updated.
     *
     * This method is always invoked from a synchronized context
     * @param settings The updated settings for this similarity provider
     */
    protected abstract void doUpdateSettings(Settings settings);

    final synchronized void updateSettings(Settings settings) {
        if (settings.equals(currentSettings)) {
            this.currentSettings = settings;
            return ;
        }
        doUpdateSettings(settings);
        this.currentSettings = settings;
    }

    final void validateUpdateSettings(Settings newSettings) {
        if (newSettings.equals(currentSettings)) {
            return ;
        }
        validateSettings(newSettings);
        settingMap.forEach((k, v) -> {
            Setting<?> setting = settingMap.get(k);
            if (setting.isDynamic() == false) {
                Object obj1 = setting.get(newSettings);
                Object obj2 = setting.get(currentSettings);
                boolean hasChanged = (Objects.equals(obj1, obj2) == false);
                if (hasChanged) {
                    throw new IllegalArgumentException("setting [" + setting.getKey() + "] " +
                        "for similarity [" + name() + "], not dynamically updateable");
                }
            }
        });
        doValidateUpdateSettings(newSettings);
    }

    final void validateSettings(Settings settings) {
        settings.getAsMap().forEach((k, v) -> {
            Setting<?> setting = settingMap.get(k);
            if (setting == null) {
                LevensteinDistance ld = new LevensteinDistance();
                List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
                for (String key : settingMap.keySet()) {
                    float distance = ld.getDistance(k, key);
                    if (distance > 0.7f) {
                        scoredKeys.add(new Tuple<>(distance, key));
                    }
                }
                CollectionUtil.timSort(scoredKeys, (a, b) -> b.v1().compareTo(a.v1()));
                String msg = "unknown setting [" + k + "] for similarity [" + name() + "]";
                List<String> keys = scoredKeys.stream().map((a) -> a.v2()).collect(Collectors.toList());
                if (keys.isEmpty() == false) {
                    msg += " did you mean " +
                        (keys.size() == 1 ? "[" + keys.get(0) + "]": "any of " + keys.toString()) + "?";
                } else {
                    msg += " please check that any required plugins are installed, " +
                        "or check the breaking changes documentation for removed settings";
                }
                throw new IllegalArgumentException(msg);
            }
            setting.get(settings);
        });
    }
}
