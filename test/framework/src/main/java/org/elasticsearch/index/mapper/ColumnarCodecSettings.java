/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;

/**
 * Names which of a columnar index's doc-values layouts a test means, rather than leaving it to the default.
 *
 * <p>The default is not the same everywhere: the ColumNAR codec stores a columnar index's doc values where its feature flag is on, and
 * the layouts it replaces are written where it is off. A test that reads one of them back therefore says which it means, and says it
 * through here so that a build writing no codec at all simply writes the layouts it replaces.
 */
public final class ColumnarCodecSettings {

    /** Whether the ColumNAR codec can store an index's doc values at all, which its feature flag decides. */
    public static final boolean AVAILABLE = ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled();

    private ColumnarCodecSettings() {}

    /**
     * Names whether the ColumNAR codec stores this index's doc values. The setting exists only alongside the codec, so where there is
     * none this adds nothing and the index is written in the layouts the codec replaces.
     */
    public static Settings.Builder name(Settings.Builder settings, boolean codec) {
        if (AVAILABLE) {
            settings.put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), codec);
        }
        return settings;
    }

    /** The layouts the ColumNAR codec replaces, for a test that reads one of them back. */
    public static Settings.Builder withoutCodec(Settings.Builder settings) {
        return name(settings, false);
    }

    /** The ColumNAR codec's own payload, for a test that reads it back. Only reachable where {@link #AVAILABLE}. */
    public static Settings.Builder withCodec(Settings.Builder settings) {
        return name(settings, true);
    }
}
