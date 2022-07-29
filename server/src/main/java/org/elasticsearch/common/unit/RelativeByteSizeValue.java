/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;

/**
 * A byte size value that allows specification using either of:
 * 1. Absolute value (200GB for instance)
 * 2. Relative percentage value (95%)
 * 3. Relative ratio value (0.95)
 */
public class RelativeByteSizeValue {

    private final ByteSizeValue absolute;
    private final RatioValue ratio;

    public RelativeByteSizeValue(ByteSizeValue absolute) {
        this.absolute = absolute;
        this.ratio = null;
    }

    public RelativeByteSizeValue(RatioValue ratio) {
        this.absolute = null;
        this.ratio = ratio;
    }

    public boolean isAbsolute() {
        return absolute != null;
    }

    public ByteSizeValue getAbsolute() {
        return absolute;
    }

    public RatioValue getRatio() {
        return ratio;
    }

    /**
     * Calculate the size to use, optionally catering for a max headroom.
     * @param total the total size to use
     * @param maxHeadroom the max headroom to cater for or null (or -1) to ignore.
     * @return the size to use
     */
    public ByteSizeValue calculateValue(ByteSizeValue total, ByteSizeValue maxHeadroom) {
        if (ratio != null) {
            long ratioBytes = (long) Math.ceil(ratio.getAsRatio() * total.getBytes());
            if (maxHeadroom != null && maxHeadroom.getBytes() != -1) {
                return ByteSizeValue.ofBytes(Math.max(ratioBytes, total.getBytes() - maxHeadroom.getBytes()));
            } else {
                return ByteSizeValue.ofBytes(ratioBytes);
            }
        } else {
            return absolute;
        }
    }

    public boolean isNonZeroSize() {
        if (ratio != null) {
            return ratio.getAsRatio() > 0.0d;
        } else {
            return absolute.getBytes() > 0;
        }
    }

    public static RelativeByteSizeValue parseRelativeByteSizeValue(String value, String settingName) {
        // If the value ends with a `b`, it implies it is probably a byte size value, so do not try to parse as a ratio/percentage at all.
        // The main motivation is to make parsing faster. Using exception throwing and catching when trying to parse as a ratio as a
        // means of identifying that a string is not a ratio can be quite slow.
        if (value.endsWith("b") == false) {
            try {
                RatioValue ratio = RatioValue.parseRatioValue(value);
                if (ratio.getAsPercent() != 0.0d || value.endsWith("%")) {
                    return new RelativeByteSizeValue(ratio);
                } else {
                    return new RelativeByteSizeValue(ByteSizeValue.ZERO);
                }
            } catch (ElasticsearchParseException e) {
                // ignore, see if it parses as bytes
            }
        }
        try {
            return new RelativeByteSizeValue(ByteSizeValue.parseBytesSizeValue(value, settingName));
        } catch (ElasticsearchParseException e) {
            throw new ElasticsearchParseException("unable to parse [{}={}] as either percentage or bytes", e, settingName, value);
        }
    }

    public String getStringRep() {
        if (ratio != null) {
            return ratio.formatNoTrailingZerosPercent();
        } else {
            return absolute.getStringRep();
        }
    }
}
