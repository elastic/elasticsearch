/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchParseException;

import java.io.IOException;

/**
 * A byte size value that allows specification using either of:
 * 1. Absolute value (200GB for instance)
 * 2. Relative percentage value (95%)
 * 3. Relative ratio value (0.95)
 */
public class RelativeByteSizeValue implements Writeable {

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
     * If a ratio/percentage is used, the resulting bytes are rounded to the next integer value.
     * @param total the total size to use
     * @param maxHeadroom the max headroom to cater for or null (or -1) to ignore.
     * @return the size to use
     */
    public ByteSizeValue calculateValue(ByteSizeValue total, ByteSizeValue maxHeadroom) {
        if (ratio != null) {
            // Use percentage instead of ratio, and divide bytes by 100, to make the calculation with double more accurate.
            double res = total.getBytes() * ratio.getAsPercent() / 100;
            long ratioBytes = (long) Math.ceil(res);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isAbsolute());
        if (isAbsolute()) {
            assert absolute != null;
            absolute.writeTo(out);
        } else {
            assert ratio != null;
            ratio.writeTo(out);
        }
    }

    public static RelativeByteSizeValue readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new RelativeByteSizeValue(ByteSizeValue.readFrom(in));
        } else {
            return new RelativeByteSizeValue(RatioValue.readFrom(in));
        }
    }
}
