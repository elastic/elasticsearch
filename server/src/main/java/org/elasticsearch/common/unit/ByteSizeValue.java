/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.BYTE_SIZE_VALUE_ALWAYS_USES_BYTES;
import static org.elasticsearch.TransportVersions.BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1;
import static org.elasticsearch.TransportVersions.REVERT_BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1;
import static org.elasticsearch.TransportVersions.V_9_0_0;
import static org.elasticsearch.common.unit.ByteSizeUnit.BYTES;
import static org.elasticsearch.common.unit.ByteSizeUnit.GB;
import static org.elasticsearch.common.unit.ByteSizeUnit.KB;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.unit.ByteSizeUnit.PB;
import static org.elasticsearch.common.unit.ByteSizeUnit.TB;

public class ByteSizeValue implements Writeable, Comparable<ByteSizeValue>, ToXContentFragment {

    /**
     * We have to lazy initialize the deprecation logger as otherwise a static logger here would be constructed before logging is configured
     * leading to a runtime failure (see {@code LogConfigurator.checkErrorListener()} ). The premature construction would come from any
     * {@link ByteSizeValue} object constructed in, for example, settings in {@link org.elasticsearch.common.network.NetworkService}.
     */
    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ByteSizeValue.class);
    }

    public static final ByteSizeValue ZERO = new ByteSizeValue(0, BYTES);
    public static final ByteSizeValue ONE = new ByteSizeValue(1, BYTES);
    public static final ByteSizeValue MINUS_ONE = new ByteSizeValue(-1, BYTES);

    /**
     * @param size the number of {@code unit}s
     */
    public static ByteSizeValue of(long size, ByteSizeUnit unit) {
        if (size < -1 || (size == -1 && unit != BYTES)) {
            throw new IllegalArgumentException("Values less than -1 bytes are not supported: " + size + unit.getSuffix());
        }
        if (size > Long.MAX_VALUE / unit.toBytes(1)) {
            throw new IllegalArgumentException(
                "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix()
            );
        }
        return newByteSizeValue(size * unit.toBytes(1), unit);
    }

    public static ByteSizeValue ofBytes(long size) {
        return of(size, BYTES);
    }

    public static ByteSizeValue ofKb(long size) {
        return of(size, KB);
    }

    public static ByteSizeValue ofMb(long size) {
        return of(size, MB);
    }

    public static ByteSizeValue ofGb(long size) {
        return of(size, GB);
    }

    public static ByteSizeValue ofTb(long size) {
        return of(size, TB);
    }

    public static ByteSizeValue ofPb(long size) {
        return of(size, PB);
    }

    static ByteSizeValue newByteSizeValue(long sizeInBytes, ByteSizeUnit desiredUnit) {
        // Peel off some common cases to avoid allocations
        if (desiredUnit == BYTES) {
            if (sizeInBytes == 0) {
                return ZERO;
            }
            if (sizeInBytes == 1) {
                return ONE;
            }
            if (sizeInBytes == -1) {
                return MINUS_ONE;
            }
        }
        if (sizeInBytes < 0) {
            throw new IllegalArgumentException("Values less than -1 bytes are not supported: " + sizeInBytes);
        }
        return new ByteSizeValue(sizeInBytes, desiredUnit);
    }

    private final long sizeInBytes;
    private final ByteSizeUnit desiredUnit;

    public static ByteSizeValue readFrom(StreamInput in) throws IOException {
        long size = in.readZLong();
        ByteSizeUnit unit = ByteSizeUnit.readFrom(in);
        if (alwaysUseBytes(in.getTransportVersion())) {
            return newByteSizeValue(size, unit);
        } else {
            return of(size, unit);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (alwaysUseBytes(out.getTransportVersion())) {
            out.writeZLong(sizeInBytes);
        } else {
            out.writeZLong(Math.divideExact(sizeInBytes, desiredUnit.toBytes(1)));
        }
        desiredUnit.writeTo(out);
    }

    private static boolean alwaysUseBytes(TransportVersion tv) {
        return tv.onOrAfter(BYTE_SIZE_VALUE_ALWAYS_USES_BYTES)
            || tv.isPatchFrom(V_9_0_0)
            || tv.between(BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1, REVERT_BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1);
    }

    ByteSizeValue(long sizeInBytes, ByteSizeUnit desiredUnit) {
        this.sizeInBytes = sizeInBytes;
        this.desiredUnit = desiredUnit;
    }

    // For testing
    long getSizeInBytes() {
        return sizeInBytes;
    }

    // For testing
    ByteSizeUnit getDesiredUnit() {
        return desiredUnit;
    }

    @Deprecated
    public int bytesAsInt() {
        long bytes = getBytes();
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size [" + toString() + "] is bigger than max int");
        }
        return (int) bytes;
    }

    public long getBytes() {
        return sizeInBytes;
    }

    public long getKb() {
        return getBytes() / KB.toBytes(1);
    }

    public long getMb() {
        return getBytes() / MB.toBytes(1);
    }

    public long getGb() {
        return getBytes() / GB.toBytes(1);
    }

    public long getTb() {
        return getBytes() / TB.toBytes(1);
    }

    public long getPb() {
        return getBytes() / PB.toBytes(1);
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
        long numUnits = sizeInBytes / desiredUnit.toBytes(1);
        long residue = sizeInBytes % desiredUnit.toBytes(1);
        if (residue == 0) {
            return numUnits + desiredUnit.getSuffix();
        } else {
            return sizeInBytes + BYTES.getSuffix();
        }
    }

    /**
     * @return a string with at most one decimal point whose magnitude is close to {@code this}.
     */
    @Override
    public String toString() {
        long bytes = getBytes();
        double value = bytes;
        String suffix = BYTES.getSuffix();
        if (bytes >= ByteSizeUnit.C5) {
            value = getPbFrac();
            suffix = PB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C4) {
            value = getTbFrac();
            suffix = TB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C3) {
            value = getGbFrac();
            suffix = GB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C2) {
            value = getMbFrac();
            suffix = MB.getSuffix();
        } else if (bytes >= ByteSizeUnit.C1) {
            value = getKbFrac();
            suffix = KB.getSuffix();
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
            return parse(sValue, lowerSValue, "k", KB, settingName);
        } else if (lowerSValue.endsWith("kb")) {
            return parse(sValue, lowerSValue, "kb", KB, settingName);
        } else if (lowerSValue.endsWith("m")) {
            return parse(sValue, lowerSValue, "m", MB, settingName);
        } else if (lowerSValue.endsWith("mb")) {
            return parse(sValue, lowerSValue, "mb", MB, settingName);
        } else if (lowerSValue.endsWith("g")) {
            return parse(sValue, lowerSValue, "g", GB, settingName);
        } else if (lowerSValue.endsWith("gb")) {
            return parse(sValue, lowerSValue, "gb", GB, settingName);
        } else if (lowerSValue.endsWith("t")) {
            return parse(sValue, lowerSValue, "t", TB, settingName);
        } else if (lowerSValue.endsWith("tb")) {
            return parse(sValue, lowerSValue, "tb", TB, settingName);
        } else if (lowerSValue.endsWith("p")) {
            return parse(sValue, lowerSValue, "p", PB, settingName);
        } else if (lowerSValue.endsWith("pb")) {
            return parse(sValue, lowerSValue, "pb", PB, settingName);
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
        assert unit != BYTES : "Use parseBytes";
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            try {
                return of(Long.parseLong(s), unit);
            } catch (final NumberFormatException e) {
                // If it's not an integer, it could be a valid number with a decimal
                BigDecimal decimalValue = parseDecimal(s, settingName, initialInput, e);
                long sizeInBytes = convertToBytes(decimalValue, unit, settingName, initialInput, e);
                return new ByteSizeValue(sizeInBytes, unit);
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

    /**
     * @param numericPortion the number to parse
     * @param settingName for error reporting - the name of the setting we're parsing
     * @param settingValue for error reporting - the whole string value of the setting
     * @param originalException for error reporting - the exception that occurred when we tried to parse the setting as an integer
     */
    private static BigDecimal parseDecimal(
        String numericPortion,
        String settingName,
        String settingValue,
        NumberFormatException originalException
    ) {
        BigDecimal decimalValue;
        try {
            decimalValue = new BigDecimal(numericPortion);
        } catch (NumberFormatException e) {
            // Here, we choose to use originalException as the cause, because a NumberFormatException here
            // indicates the string wasn't actually a valid BigDecimal after all, so there's no reason
            // to confuse matters by reporting BigDecimal in the stack trace.
            ElasticsearchParseException toThrow = new ElasticsearchParseException(
                "failed to parse setting [{}] with value [{}]",
                originalException,
                settingName,
                settingValue
            );
            toThrow.addSuppressed(e);
            throw toThrow;
        }
        if (decimalValue.signum() < 0) {
            throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}]", settingName, settingValue);
        } else if (decimalValue.scale() > 2) {
            throw new ElasticsearchParseException(
                "failed to parse setting [{}] with more than two decimals in value [{}]",
                settingName,
                settingValue
            );
        }
        return decimalValue;
    }

    /**
     * @param decimalValue the number of {@code unit}s
     * @param unit the specified {@link ByteSizeUnit}
     * @param settingName for error reporting - the name of the setting we're parsing
     * @param settingValue for error reporting - the whole string value of the setting
     * @param originalException for error reporting - the exception that occurred when we tried to parse the setting as an integer
     */
    private static long convertToBytes(
        BigDecimal decimalValue,
        ByteSizeUnit unit,
        String settingName,
        String settingValue,
        NumberFormatException originalException
    ) {
        BigDecimal sizeInBytes = decimalValue.multiply(new BigDecimal(unit.toBytes(1)));
        try {
            // Note we always round up here for two reasons:
            // 1. Practically: toString truncates, so if we ever round down, we'll lose a tenth
            // 2. In principle: if the user asks for 1.1kb, which is 1126.4 bytes, and we only give then 1126, then
            // we have not given them what they asked for.
            return sizeInBytes.setScale(0, RoundingMode.UP).longValueExact();
        } catch (ArithmeticException e) {
            // Here, we choose to use the ArithmeticException as the cause, because we already know the
            // number is a valid BigDecimal, so it makes sense to supply that context in the stack trace.
            ElasticsearchParseException toThrow = new ElasticsearchParseException(
                "failed to parse setting [{}] with value beyond {}: [{}]",
                e,
                settingName,
                Long.MAX_VALUE,
                settingValue
            );
            toThrow.addSuppressed(originalException);
            throw toThrow;
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
        return Long.hashCode(getBytes());
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
