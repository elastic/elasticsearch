/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * A parser that parses and validates disk thresholds. The disk thresholds accept two different type of inputs either bytes or percentages.
 * The percentage usually refers to the maximum disk usage and the bytes to the minimum free disk space. Furthermore, in the same type of
 * thresholds there is an implied order, for example:
 * - health thresholds yellow represents lower disk usage than red
 * - allocation watermarks low represents lower disk usage than high and flood stage.
 * This parser provides helper functions to handle all the above cases.
 */
public class DiskThresholdParser {

    /**
     * Helper class to pair setting name and raw value.
     * @param name the name of the setting
     * @param value the raw value of the threshold
     */
    public record ThresholdSetting(String name, String value) {
        @Override
        public String toString() {
            return String.format("[%s=%s]", name, value);
        }
    }

    /**
     * Attempts to parse the threshold into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    public static double thresholdPercentageFromThreshold(String threshold) {
        return thresholdPercentageFromThreshold(threshold, true);
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
    private static double thresholdPercentageFromThreshold(String threshold, boolean lenient) {
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
    public static ByteSizeValue thresholdBytesFromThreshold(String threshold, String settingName) {
        return thresholdBytesFromThreshold(threshold, settingName, true);
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
    private static ByteSizeValue thresholdBytesFromThreshold(String threshold, String settingName, boolean lenient) {
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

    /**
     * Checks if a threshold string is a valid percentage or byte size value,
     * @return the threshold value given
     */
    public static String validThresholdSetting(String threshold, String settingName) {
        if (definitelyNotPercentage(threshold)) {
            // short circuit to save expensive exception on obvious byte size value below
            ByteSizeValue.parseBytesSizeValue(threshold, settingName);
            return threshold;
        }
        try {
            RatioValue.parseRatioValue(threshold);
        } catch (ElasticsearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(threshold, settingName);
            } catch (ElasticsearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return threshold;
    }

    // Checks that a value is definitely not a percentage by testing if it ends on `b` which implies that it is probably a byte size value
    // instead. This is used to make setting validation skip attempting to parse a value as a percentage/ration for the settings in this
    // class that accept either a byte size value. The main motivation of this method is to make tests faster. Some tests call this method
    // frequently when starting up internal cluster nodes and using exception throwing and catching when trying to parse as a ratio as a
    // means of identifying that a string is not a ratio is quite slow.
    public static boolean definitelyNotPercentage(String value) {
        return value.endsWith("b");
    }

    public static void doValidate(String newValue, List<ThresholdSetting> thresholdsInOrder) {
        // only try to validate as percentage if it isn't obviously a byte size value
        if (definitelyNotPercentage(newValue) == false) {
            try {
                doValidateAsPercentage(thresholdsInOrder);
                return; // early return so that we do not try to parse as bytes
            } catch (final ElasticsearchParseException e) {
                // swallow as we are now going to try to parse as bytes
            }
        }
        try {
            doValidateAsBytes(thresholdsInOrder);
        } catch (final ElasticsearchParseException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to consistently parse %s as percentage or bytes",
                thresholdsInOrder.stream().map(ThresholdSetting::toString).collect(Collectors.joining(", "))
            );
            throw new IllegalArgumentException(message, e);
        }
    }

    public static void doValidateAsPercentage(List<ThresholdSetting> thresholdsInOrder) {
        for (int i = 1; i < thresholdsInOrder.size(); i++) {
            ThresholdSetting lowerThreshold = thresholdsInOrder.get(i - 1);
            ThresholdSetting higherThreshold = thresholdsInOrder.get(i);
            if (thresholdPercentageFromThreshold(lowerThreshold.value, false) > thresholdPercentageFromThreshold(
                higherThreshold.value,
                false
            )) {
                final String message = String.format(Locale.ROOT, "setting %s cannot be greater than %s", lowerThreshold, higherThreshold);
                throw new IllegalArgumentException(message);
            }
        }
    }

    public static void doValidateAsBytes(List<ThresholdSetting> thresholdsInOrder) {
        for (int i = 1; i < thresholdsInOrder.size(); i++) {
            ThresholdSetting lowerThreshold = thresholdsInOrder.get(i - 1);
            ThresholdSetting higherThreshold = thresholdsInOrder.get(i);
            if (thresholdBytesFromThreshold(lowerThreshold.value, lowerThreshold.name, false).getBytes() < thresholdBytesFromThreshold(
                higherThreshold.value,
                higherThreshold.name,
                false
            ).getBytes()) {
                final String message = String.format(Locale.ROOT, "setting %s cannot be less than %s", lowerThreshold, higherThreshold);
                throw new IllegalArgumentException(message);
            }
        }
    }
}
