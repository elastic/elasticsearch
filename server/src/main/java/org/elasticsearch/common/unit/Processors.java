/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public class Processors implements Writeable, Comparable<Processors>, ToXContentFragment {
    public static final Processors ZERO = new Processors(0.0);
    public static final Processors MAX_PROCESSORS = new Processors(Double.MAX_VALUE);

    public static final Version FLOAT_PROCESSORS_SUPPORT_VERSION = Version.V_8_3_0;
    public static final Version DOUBLE_PROCESSORS_SUPPORT_VERSION = Version.V_8_5_0;
    static final int NUMBER_OF_DECIMAL_PLACES = 5;
    private static final double MIN_REPRESENTABLE_PROCESSORS = 1E-5;

    private final double count;

    private Processors(double count) {
        // Avoid rounding up to MIN_REPRESENTABLE_PROCESSORS when 0 processors are used
        if (count == 0.0) {
            this.count = count;
        } else {
            this.count = Math.max(
                MIN_REPRESENTABLE_PROCESSORS,
                new BigDecimal(count).setScale(NUMBER_OF_DECIMAL_PLACES, RoundingMode.HALF_UP).doubleValue()
            );
        }
    }

    @Nullable
    public static Processors of(Double count) {
        if (count == null) {
            return null;
        }

        if (validNumberOfProcessors(count) == false) {
            throw new IllegalArgumentException("processors must be a non-negative number; provided [" + count + "]");
        }

        return new Processors(count);
    }

    public static Processors readFrom(StreamInput in) throws IOException {
        final double processorCount;
        if (in.getVersion().before(FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            processorCount = in.readInt();
        } else if (in.getVersion().before(DOUBLE_PROCESSORS_SUPPORT_VERSION)) {
            processorCount = in.readFloat();
        } else {
            processorCount = in.readDouble();
        }
        return new Processors(processorCount);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            assert hasDecimals() == false;
            out.writeInt((int) count);
        } else if (out.getVersion().before(DOUBLE_PROCESSORS_SUPPORT_VERSION)) {
            out.writeFloat((float) count);
        } else {
            out.writeDouble(count);
        }
    }

    @Nullable
    public static Processors fromXContent(XContentParser parser) throws IOException {
        final double count = parser.doubleValue();
        if (validNumberOfProcessors(count) == false) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "Only a positive number of [%s] are allowed and [%f] was provided", parser.currentName(), count)
            );
        }
        return new Processors(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(count);
    }

    public Processors plus(Processors other) {
        final double newProcessorCount = count + other.count;
        if (Double.isFinite(newProcessorCount) == false) {
            throw new ArithmeticException("Unable to add [" + this + "] and [" + other + "] the resulting value overflows");
        }

        return new Processors(newProcessorCount);
    }

    public Processors multiply(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Processors cannot be multiplied by a negative number");
        }

        final double newProcessorCount = count * value;
        if (Double.isFinite(newProcessorCount) == false) {
            throw new ArithmeticException("Unable to multiply [" + this + "] by [" + value + "] the resulting value overflows");
        }

        return new Processors(newProcessorCount);
    }

    public double count() {
        return count;
    }

    public int roundUp() {
        return (int) Math.ceil(count);
    }

    public int roundDown() {
        return Math.max(1, (int) Math.floor(count));
    }

    private static boolean validNumberOfProcessors(double processors) {
        return Double.isFinite(processors) && processors > 0.0;
    }

    private boolean hasDecimals() {
        return ((int) count) != Math.ceil(count);
    }

    public boolean isCompatibleWithVersion(Version version) {
        if (version.onOrAfter(FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            return true;
        }

        return hasDecimals() == false;
    }

    @Override
    public int compareTo(Processors o) {
        return Double.compare(count, o.count);
    }

    public static boolean equalsOrCloseTo(Processors a, Processors b) {
        return (a == b) || (a != null && (a.equals(b) || a.closeToAsFloat(b)));
    }

    private boolean closeToAsFloat(Processors b) {
        if (b == null) {
            return false;
        }

        float floatCount = (float) count;
        float otherFloatCount = (float) b.count;
        float maxError = Math.max(Math.ulp(floatCount), Math.ulp(otherFloatCount)) + (float) MIN_REPRESENTABLE_PROCESSORS;
        return Float.isFinite(floatCount) && Float.isFinite(otherFloatCount) && (Math.abs(floatCount - otherFloatCount) < maxError);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Processors that = (Processors) o;
        return Double.compare(that.count, count) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public String toString() {
        return Double.toString(count);
    }
}
