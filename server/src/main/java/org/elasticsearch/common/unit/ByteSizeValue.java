/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeUnit.BYTES;

public class ByteSizeValue implements Writeable, Comparable<ByteSizeValue>, ToXContentFragment {

    /**
     * We have to lazy initialize the deprecation logger as otherwise a static logger here would be constructed before logging is configured
     * leading to a runtime failure (see {@code LogConfigurator.checkErrorListener()} ). The premature construction would come from any
     * {@link ByteSizeValue} object constructed in, for example, settings in {@link org.elasticsearch.common.network.NetworkService}.
     */
    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ByteSizeValue.class);
    }

    public static final ByteSizeValue ZERO = of(0, BYTES);
    public static final ByteSizeValue ONE = of(1, BYTES);
    public static final ByteSizeValue MINUS_ONE = of(-1, BYTES);

    /**
     * @param size denominated in the given {@code unit}
     */
    public static ByteSizeValue of(long size, ByteSizeUnit unit) {
        return new ByteSizeValue(size, unit);
    }

    /**
     * @return {@link ByteSizeValue} whose {@link #preferredUnit} is a reasonable one for human consumption.
     */
    public static ByteSizeValue withAutomaticUnit(long sizeInBytes) {
        if (sizeInBytes == 0) {
            return ZERO;
        }

        // We pick a unit such that sizeInBytes is a multiple of 1/4 of that unit.
        // That preserves the exact given number of bytes without using more than 2 decimal places.
        for (int ordinal = ByteSizeUnit.values().length - 1; ordinal >= 0; --ordinal) {
            ByteSizeUnit candidateUnit = ByteSizeUnit.values()[ordinal];
            if (candidateUnit == BYTES) {
                // We handle this using ofBytes below
                continue;
            }
            if (sizeInBytes % (candidateUnit.toBytes(1) / 4) == 0) {
                return new ByteSizeValue(sizeInBytes, candidateUnit, 0xdead);
            }
        }
        return ofBytes(sizeInBytes);
    }

    public static ByteSizeValue ofBytes(long size) {
        if (size == 0) {
            return ZERO;
        }
        if (size == 1) {
            return ONE;
        }
        if (size == -1) {
            return MINUS_ONE;
        }
        return of(size, BYTES);
    }

    public static ByteSizeValue ofKb(long size) {
        return of(size, ByteSizeUnit.KB);
    }

    public static ByteSizeValue ofMb(long size) {
        return of(size, ByteSizeUnit.MB);
    }

    public static ByteSizeValue ofGb(long size) {
        return of(size, ByteSizeUnit.GB);
    }

    public static ByteSizeValue ofTb(long size) {
        return of(size, ByteSizeUnit.TB);
    }

    public static ByteSizeValue ofPb(long size) {
        return of(size, ByteSizeUnit.PB);
    }

    final long sizeInBytes;
    final ByteSizeUnit preferredUnit;

    public static ByteSizeValue readFrom(StreamInput in) throws IOException {
        long size = in.readZLong();
        ByteSizeUnit unit = ByteSizeUnit.readFrom(in);
        if (unit == BYTES) {
            return ofBytes(size);
        }
        return of(size, unit);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The 8.x behaviour is:
        // 1. send the value denominated in the given unit, but...
        // 2. if the value given to the parser was a fraction, its unit was ignored and we use BYTES
        //
        // We emulate that behaviour "retroactively" here by treating fractional values the same way
        boolean isWholeNumber = (sizeInBytes % preferredUnit.toBytes(1)) == 0;
        if (isWholeNumber) {
            out.writeZLong(sizeInBytes / preferredUnit.toBytes(1));
            preferredUnit.writeTo(out);
        } else {
            out.writeZLong(sizeInBytes);
            BYTES.writeTo(out);
        }
    }

    /**
     * @deprecated use {@link #of}.
     */
    @Deprecated(forRemoval = true)
    public ByteSizeValue(long size, ByteSizeUnit unit) {
        if (size < -1 || (size == -1 && unit != BYTES)) {
            throw new IllegalArgumentException("Values less than -1 bytes are not supported: " + size + unit.getSuffix());
        }
        if (size > Long.MAX_VALUE / unit.toBytes(1)) {
            throw new IllegalArgumentException(
                "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix()
            );
        }
        this.sizeInBytes = unit.toBytes(size);
        this.preferredUnit = unit;
    }

    /**
     * @param ignored distinguishes this overload from {@link ByteSizeValue#ByteSizeValue(long, ByteSizeUnit)}
     */
    ByteSizeValue(long sizeInBytes, ByteSizeUnit preferredUnit, int ignored) {
        this.sizeInBytes = sizeInBytes;
        this.preferredUnit = preferredUnit;
    }

    @Deprecated
    public int bytesAsInt() {
        long bytes = getBytes();
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size [" + this + "] is bigger than max int");
        }
        return (int) bytes;
    }

    public long getBytes() {
        return sizeInBytes;
    }

    public long getKb() {
        return BYTES.toKB(sizeInBytes);
    }

    public long getMb() {
        return BYTES.toMB(sizeInBytes);
    }

    public long getGb() {
        return BYTES.toGB(sizeInBytes);
    }

    public long getTb() {
        return BYTES.toTB(sizeInBytes);
    }

    public long getPb() {
        return BYTES.toPB(sizeInBytes);
    }

    public double getKbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C1;
    }

    public double getMbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C2;
    }

    public double getGbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C3;
    }

    public double getTbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C4;
    }

    public double getPbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C5;
    }

    /**
     * @return a string representation of this value which is guaranteed to be
     *         able to be parsed using
     *         {@link #parseBytesSizeValue(String, ByteSizeValue, String)}.
     *         Unlike {@link #toString()} this method will not output fractional
     *         or rounded values so this method should be preferred when
     *         serialising the value to JSON.
     */
    public String getStringRep() {
        if (sizeInBytes <= 0) {
            return String.valueOf(sizeInBytes);
        }
        if (preferredUnit == BYTES) {
            return sizeInBytes + BYTES.getSuffix();
        }
        long fractionalPart = sizeInBytes % preferredUnit.toBytes(1);
        if (fractionalPart == 0) {
            // Already an integer number of the preferred unit
            return sizeInBytes / preferredUnit.toBytes(1) + preferredUnit.getSuffix();
        } else if (fractionalPart % (preferredUnit.toBytes(1) / 1024) == 0) {
            // For some fractions like, we can use the next unit down, which is preferable to bytes.
            // For example, 0.75 TB can be "768 GB" instead of "805306368 B".
            var smallerUnit = ByteSizeUnit.values()[preferredUnit.ordinal() - 1];
            return sizeInBytes / smallerUnit.toBytes(1) + smallerUnit.getSuffix();
        } else {
            // It's hopeless: no suffix besides BYTES can make this an integer.
            // For example, 0.33 GB = 337.92 MB = 346030.08 KB.
            // Bytes are special because we always round to the nearest byte anyway.
            return sizeInBytes + BYTES.getSuffix();
        }
    }

    @Override
    public String toString() {
        long bytes = getBytes();
        double value = bytes;
        String suffix = ByteSizeUnit.BYTES.getSuffix();
        if (bytes >= ByteSizeUnit.C5) {
            value = getPbFrac();
            suffix = ByteSizeUnit.PB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C4) {
            value = getTbFrac();
            suffix = ByteSizeUnit.TB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C3) {
            value = getGbFrac();
            suffix = ByteSizeUnit.GB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C2) {
            value = getMbFrac();
            suffix = ByteSizeUnit.MB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C1) {
            value = getKbFrac();
            suffix = ByteSizeUnit.KB.getSuffix();
        }
        return Strings.format1Decimals(value, suffix);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, String settingName) throws ElasticsearchParseException {
        return parseBytesSizeValue(sValue, null, settingName);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, ByteSizeValue defaultValue, String settingName)
        throws ElasticsearchParseException {
        settingName = Objects.requireNonNull(settingName);
        if (sValue == null) {
            return defaultValue;
        }
        switch (sValue) {
            case "0":
            case "0b":
            case "0B":
                return ZERO;
            case "1b":
            case "1B":
                // "1" is deliberately omitted, the units are required for all values except "0" and "-1"
                return ONE;
            case "-1":
            case "-1b":
            case "-1B":
                return MINUS_ONE;
        }
        String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
        if (lowerSValue.endsWith("k")) {
            return parse(sValue, lowerSValue, "k", ByteSizeUnit.KB, settingName);
        } else if (lowerSValue.endsWith("kb")) {
            return parse(sValue, lowerSValue, "kb", ByteSizeUnit.KB, settingName);
        } else if (lowerSValue.endsWith("m")) {
            return parse(sValue, lowerSValue, "m", ByteSizeUnit.MB, settingName);
        } else if (lowerSValue.endsWith("mb")) {
            return parse(sValue, lowerSValue, "mb", ByteSizeUnit.MB, settingName);
        } else if (lowerSValue.endsWith("g")) {
            return parse(sValue, lowerSValue, "g", ByteSizeUnit.GB, settingName);
        } else if (lowerSValue.endsWith("gb")) {
            return parse(sValue, lowerSValue, "gb", ByteSizeUnit.GB, settingName);
        } else if (lowerSValue.endsWith("t")) {
            return parse(sValue, lowerSValue, "t", ByteSizeUnit.TB, settingName);
        } else if (lowerSValue.endsWith("tb")) {
            return parse(sValue, lowerSValue, "tb", ByteSizeUnit.TB, settingName);
        } else if (lowerSValue.endsWith("p")) {
            return parse(sValue, lowerSValue, "p", ByteSizeUnit.PB, settingName);
        } else if (lowerSValue.endsWith("pb")) {
            return parse(sValue, lowerSValue, "pb", ByteSizeUnit.PB, settingName);
        } else if (lowerSValue.endsWith("b")) {
            return parseBytes(lowerSValue, settingName, sValue);
        } else {
            // Missing units:
            throw new ElasticsearchParseException(
                "failed to parse setting [{}] with value [{}] as a size in bytes: unit is missing or unrecognized",
                settingName,
                sValue
            );
        }
    }

    private static ByteSizeValue parseBytes(String lowerSValue, String settingName, String initialInput) {
        String s = lowerSValue.substring(0, lowerSValue.length() - 1).trim();
        try {
            return ByteSizeValue.ofBytes(Long.parseLong(s));
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}]", e, settingName, initialInput);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException(
                "failed to parse setting [{}] with value [{}] as a size in bytes",
                e,
                settingName,
                initialInput
            );
        }
    }

    private static ByteSizeValue parse(
        final String initialInput,
        final String normalized,
        final String suffix,
        ByteSizeUnit unit,
        final String settingName
    ) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            try {
                return of(Long.parseLong(s), unit);
            } catch (final NumberFormatException e) {
                try {
                    BigDecimal decimalValue = new BigDecimal(s);
                    if (decimalValue.scale() > 2) {
                        throw new ElasticsearchParseException(
                            "more than two decimal places in setting [{}] with value [{}]",
                            settingName,
                            initialInput
                        );
                    }
                    return new ByteSizeValue(decimalValue.multiply(new BigDecimal(unit.toBytes(1))).longValue(), unit, 0xdead);
                } catch (final NumberFormatException ignored) {
                    throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}]", e, settingName, initialInput);
                }
            }
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException(
                "failed to parse setting [{}] with value [{}] as a size in bytes",
                e,
                settingName,
                initialInput
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return compareTo((ByteSizeValue) o) == 0;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(sizeInBytes);
    }

    @Override
    public int compareTo(ByteSizeValue other) {
        return Long.compare(getBytes(), other.getBytes());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }

    /**
     * @return Constructs a {@link ByteSizeValue} with the bytes resulting from the addition of the arguments' bytes. Note that the
     *         resulting {@link ByteSizeUnit} is bytes.
     * @throws IllegalArgumentException if any of the arguments have -1 bytes
     */
    public static ByteSizeValue add(ByteSizeValue x, ByteSizeValue y) {
        if (x.equals(ByteSizeValue.MINUS_ONE) || y.equals(ByteSizeValue.MINUS_ONE)) {
            throw new IllegalArgumentException("one of the arguments has -1 bytes");
        }
        return ByteSizeValue.ofBytes(Math.addExact(x.getBytes(), y.getBytes()));
    }

    /**
     * @return Constructs a {@link ByteSizeValue} with the bytes resulting from the difference of the arguments' bytes. Note that the
     *         resulting {@link ByteSizeUnit} is bytes.
     * @throws IllegalArgumentException if any of the arguments or the result have -1 bytes
     */
    public static ByteSizeValue subtract(ByteSizeValue x, ByteSizeValue y) {
        if (x.equals(ByteSizeValue.MINUS_ONE) || y.equals(ByteSizeValue.MINUS_ONE)) {
            throw new IllegalArgumentException("one of the arguments has -1 bytes");
        }
        // No need to use Math.subtractExact here, since we know both arguments are >= 0.
        ByteSizeValue res = ByteSizeValue.ofBytes(x.getBytes() - y.getBytes());
        if (res.equals(ByteSizeValue.MINUS_ONE)) {
            throw new IllegalArgumentException("subtraction result has -1 bytes");
        }
        return res;
    }

    /**
     * @return Returns the lesser of the two given {@link ByteSizeValue} arguments. In case of equality, the first argument is returned.
     * @throws IllegalArgumentException if any of the arguments have -1 bytes
     */
    public static ByteSizeValue min(ByteSizeValue x, ByteSizeValue y) {
        if (x.equals(ByteSizeValue.MINUS_ONE) || y.equals(ByteSizeValue.MINUS_ONE)) {
            throw new IllegalArgumentException("one of the arguments has -1 bytes");
        }
        return x.compareTo(y) <= 0 ? x : y;
    }
}
