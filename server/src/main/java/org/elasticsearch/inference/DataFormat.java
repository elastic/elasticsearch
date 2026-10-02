/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.FeatureFlag;

import java.util.Arrays;
import java.util.Locale;

/**
 * Describes the format of data to perform inference on
 */
public enum DataFormat {
    TEXT,
    BASE64,
    URL;

    /**
     * Feature flag for URL-format inference inputs. Gated here because {@link DataFormat#fromString} needs it to filter
     * {@link #URL} from error messages when the feature is not yet enabled.
     */
    public static final FeatureFlag URL_INPUT_FORMAT_FEATURE_FLAG = new FeatureFlag("inference_url_input_format");

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static DataFormat fromString(String name) {
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            var displayedFormats = Arrays.stream(values()).filter(f -> f != URL || URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled()).toList();
            throw new IllegalArgumentException(Strings.format("Unrecognized format [%s], must be one of %s", name, displayedFormats));
        }
    }
}
