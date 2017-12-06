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
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class ByteSizeValue implements Writeable, Comparable<ByteSizeValue> {
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(ByteSizeValue.class));

    private final long size;
    private final ByteSizeUnit unit;

    public ByteSizeValue(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_6_2_0)) {
            size = in.readVLong();
        } else {
            size = in.readZLong();
        }
        unit = ByteSizeUnit.BYTES;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_6_2_0)) {
            out.writeVLong(getBytes());
        } else {
            out.writeZLong(getBytes());
        }
    }

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit unit) {
        this.size = size;
        this.unit = unit;
        if (size > Long.MAX_VALUE / unit.toBytes(1)) {
            throw new IllegalArgumentException(
                    "Values greater than " + Long.MAX_VALUE + " bytes are not supported: " + size + unit.getSuffix());
        }
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

    public String getStringRep() {
        if (size <= 0) {
            return String.valueOf(size);
        }
        return getBytes() + ByteSizeUnit.BYTES.getSuffix();
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
        long bytes;
        String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
        if (lowerSValue.endsWith("k")) {
            bytes = (long) (parse(sValue, lowerSValue, "k", settingName) * ByteSizeUnit.C1);
        } else if (lowerSValue.endsWith("kb")) {
            bytes = (long) (parse(sValue, lowerSValue, "kb", settingName) * ByteSizeUnit.C1);
        } else if (lowerSValue.endsWith("m")) {
            bytes = (long) (parse(sValue, lowerSValue, "m", settingName) * ByteSizeUnit.C2);
        } else if (lowerSValue.endsWith("mb")) {
            bytes = (long) (parse(sValue, lowerSValue, "mb", settingName) * ByteSizeUnit.C2);
        } else if (lowerSValue.endsWith("g")) {
            bytes = (long) (parse(sValue, lowerSValue, "g", settingName) * ByteSizeUnit.C3);
        } else if (lowerSValue.endsWith("gb")) {
            bytes = (long) (parse(sValue, lowerSValue, "gb", settingName) * ByteSizeUnit.C3);
        } else if (lowerSValue.endsWith("t")) {
            bytes = (long) (parse(sValue, lowerSValue, "t", settingName) * ByteSizeUnit.C4);
        } else if (lowerSValue.endsWith("tb")) {
            bytes = (long) (parse(sValue, lowerSValue, "tb", settingName) * ByteSizeUnit.C4);
        } else if (lowerSValue.endsWith("p")) {
            bytes = (long) (parse(sValue, lowerSValue, "p", settingName) * ByteSizeUnit.C5);
        } else if (lowerSValue.endsWith("pb")) {
            bytes = (long) (parse(sValue, lowerSValue, "pb", settingName) * ByteSizeUnit.C5);
        } else if (lowerSValue.endsWith("b")) {
            bytes = Long.parseLong(lowerSValue.substring(0, lowerSValue.length() - 1).trim());
        } else if (lowerSValue.equals("-1")) {
            // Allow this special value to be unit-less:
            bytes = -1;
        } else if (lowerSValue.equals("0")) {
            // Allow this special value to be unit-less:
            bytes = 0;
        } else {
            // Missing units:
            throw new ElasticsearchParseException(
                    "failed to parse setting [{}] with value [{}] as a size in bytes: unit is missing or unrecognized", settingName,
                    sValue);
        }
        return new ByteSizeValue(bytes, ByteSizeUnit.BYTES);
    }

    private static double parse(final String initialInput, final String normalized, final String suffix, final String settingName) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            return Long.parseLong(s);
        } catch (final NumberFormatException e) {
            try {
                final double doubleValue = Double.parseDouble(s);
                DEPRECATION_LOGGER.deprecated(
                        "Fractional bytes values are deprecated. Use non-fractional bytes values instead: [{}] found for setting [{}]",
                        initialInput, settingName);
                return doubleValue;
            } catch (final NumberFormatException ignored) {
                throw new ElasticsearchParseException("failed to parse [{}]", e, initialInput);
            }
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
}
