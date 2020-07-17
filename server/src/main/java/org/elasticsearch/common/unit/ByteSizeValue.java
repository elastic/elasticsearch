/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class ByteSizeValue implements Writeable, Comparable<ByteSizeValue>, ToXContentFragment {

    /**
     * We have to lazy initialize the deprecation logger as otherwise a static logger here would be constructed before logging is configured
     * leading to a runtime failure (see {@link LogConfigurator#checkErrorListener()} ). The premature construction would come from any
     * {@link ByteSizeValue} object constructed in, for example, settings in {@link org.elasticsearch.common.network.NetworkService}.
     */
    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ByteSizeValue.class);
    }

    public static final ByteSizeValue ZERO = new ByteSizeValue(0, ByteSizeUnit.BYTES);

    private final long size;
    private final ByteSizeUnit unit;

    public ByteSizeValue(StreamInput in) throws IOException {
        size = in.readZLong();
        unit = ByteSizeUnit.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(size);
        unit.writeTo(out);
    }

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit unit) {
        if (size < -1 || (size == -1 && unit != ByteSizeUnit.BYTES)) {
            throw new IllegalArgumentException("Values less than -1 bytes are not supported: " + size + unit.getSuffix());
        }
        if (size > Long.MAX_VALUE / unit.toBytes(1)) {
            throw new IllegalArgumentException(
                    "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix());
        }
        this.size = size;
        this.unit = unit;
    }

    // For testing
    long getSize() {
        return size;
    }

    // For testing
    ByteSizeUnit getUnit() {
        return unit;
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
        return unit.toBytes(size);
    }

    public long getKb() {
        return unit.toKB(size);
    }

    public long getMb() {
        return unit.toMB(size);
    }

    public long getGb() {
        return unit.toGB(size);
    }

    public long getTb() {
        return unit.toTB(size);
    }

    public long getPb() {
        return unit.toPB(size);
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
        if (size <= 0) {
            return String.valueOf(size);
        }
        return size + unit.getSuffix();
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
            return new ByteSizeValue(Long.parseLong(lowerSValue.substring(0, lowerSValue.length() - 1).trim()), ByteSizeUnit.BYTES);
        } else if (lowerSValue.equals("-1")) {
            // Allow this special value to be unit-less:
            return new ByteSizeValue(-1, ByteSizeUnit.BYTES);
        } else if (lowerSValue.equals("0")) {
            // Allow this special value to be unit-less:
            return new ByteSizeValue(0, ByteSizeUnit.BYTES);
        } else {
            // Missing units:
            throw new ElasticsearchParseException(
                    "failed to parse setting [{}] with value [{}] as a size in bytes: unit is missing or unrecognized", settingName,
                    sValue);
        }
    }

    private static ByteSizeValue parse(final String initialInput, final String normalized, final String suffix, ByteSizeUnit unit,
            final String settingName) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            try {
                return new ByteSizeValue(Long.parseLong(s), unit);
            } catch (final NumberFormatException e) {
                try {
                    final double doubleValue = Double.parseDouble(s);
                    DeprecationLoggerHolder.deprecationLogger
                        .deprecate("fractional_byte_values",
                         "Fractional bytes values are deprecated. Use non-fractional bytes values instead: [{}] found for setting [{}]",
                         initialInput, settingName);
                    return new ByteSizeValue((long) (doubleValue * unit.toBytes(1)));
                } catch (final NumberFormatException ignored) {
                    throw new ElasticsearchParseException("failed to parse [{}]", e, initialInput);
                }
            }
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}] as a size in bytes", e, settingName,
                    initialInput);
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
        return Long.hashCode(size * unit.toBytes(1));
    }

    @Override
    public int compareTo(ByteSizeValue other) {
        long thisValue = size * unit.toBytes(1);
        long otherValue = other.size * other.unit.toBytes(1);
        return Long.compare(thisValue, otherValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
