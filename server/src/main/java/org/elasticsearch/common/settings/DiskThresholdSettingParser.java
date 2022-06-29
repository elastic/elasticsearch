/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;

/**
 * Parses disk threshold settings. The disk thresholds settings accept three different type of inputs:
 * - bytes (100gb),
 * - floating-point percentages (0.85) and
 * - percentages (85%).
 */
public class DiskThresholdSettingParser {

    /**
     * Attempts to parse the threshold into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    public static double parseThresholdPercentage(String threshold) {
        return parseThresholdPercentage(threshold, true);
    }

    /**
     * Attempts to parse the threshold into a percentage, returning 100.0% if
     * it can not be parsed and the specified lenient parameter is
     * true, otherwise throwing an {@link ElasticsearchParseException}.
     *
     * @param threshold the threshold to parse as a threshold
     * @param lenient true if lenient parsing should be applied
     * @return the parsed percentage
     */
    public static double parseThresholdPercentage(String threshold, boolean lenient) {
        if (lenient && definitelyNotPercentage(threshold)) {
            // obviously not a percentage so return lenient fallback value like we would below on a parse failure
            return 100.0;
        }
        try {
            return RatioValue.parseRatioValue(threshold).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return 100.0;
            }
            throw ex;
        }
    }

    /**
     * Attempts to parse the threshold into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    public static ByteSizeValue parseThresholdBytes(String threshold, String settingName) {
        return parseThresholdBytes(threshold, settingName, true);
    }

    /**
     * Attempts to parse the threshold into a {@link ByteSizeValue}, returning zero bytes
     * if it can not be parsed and the specified lenient parameter is true, otherwise throwing
     * an {@link ElasticsearchParseException}.
     *
     * @param threshold the threshold to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    public static ByteSizeValue parseThresholdBytes(String threshold, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(threshold, settingName);
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return ByteSizeValue.ZERO;
            }
            throw ex;
        }
    }

    // Checks that a value is definitely not a percentage by testing if it ends on `b` which implies that it is probably a byte size value
    // instead. This is used to make setting validation skip attempting to parse a value as a percentage/ration for the settings in this
    // class that accept either a byte size value. The main motivation of this method is to make tests faster. Some tests call this method
    // frequently when starting up internal cluster nodes and using exception throwing and catching when trying to parse as a ratio as a
    // means of identifying that a string is not a ratio is quite slow.
    public static boolean definitelyNotPercentage(String value) {
        return value.endsWith("b");
    }
}
