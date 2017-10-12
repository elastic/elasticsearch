/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_ENABLED_SETTING;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;

/**
 * Container class for CCR functionality.
 */
public final class Ccr {

    @SuppressWarnings("unused,FieldCanBeLocal")
    private final boolean enabled;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    public Ccr(final Settings settings) {
        this.enabled = CCR_ENABLED_SETTING.get(settings);
    }

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    public List<Setting<?>> getSettings() {
        return CcrSettings.getSettings();
    }

    /**
     * The optional engine factory for CCR. This method inspects the index settings for the {@link CcrSettings#CCR_FOLLOWING_INDEX_SETTING}
     * setting to determine whether or not the engine implementation should be a following engine.
     *
     * @return the optional engine factory
     */
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        if (CCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
            return Optional.of(new FollowingEngineFactory());
        } else {
            return Optional.empty();
        }
    }

}
