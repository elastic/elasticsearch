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

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class ByteSizeValue implements Writeable, Comparable<ByteSizeValue> {

    private final long size;
    private final ByteSizeUnit unit;

    public ByteSizeValue(StreamInput in) throws IOException {
        size = in.readVLong();
        unit = ByteSizeUnit.BYTES;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(getBytes());
    }

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit unit) {
        this.size = size;
        this.unit = unit;
    }

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

    @Override
    public String toString() {
        long bytes = getBytes();
        double value = bytes;
        String suffix = "b";
        if (bytes >= ByteSizeUnit.C5) {
            value = getPbFrac();
            suffix = "pb";
        } else if (bytes >= ByteSizeUnit.C4) {
            value = getTbFrac();
            suffix = "tb";
        } else if (bytes >= ByteSizeUnit.C3) {
            value = getGbFrac();
            suffix = "gb";
        } else if (bytes >= ByteSizeUnit.C2) {
            value = getMbFrac();
            suffix = "mb";
        } else if (bytes >= ByteSizeUnit.C1) {
            value = getKbFrac();
            suffix = "kb";
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
        try {
            String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
            if (lowerSValue.endsWith("k")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * ByteSizeUnit.C1);
            } else if (lowerSValue.endsWith("kb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * ByteSizeUnit.C1);
            } else if (lowerSValue.endsWith("m")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * ByteSizeUnit.C2);
            } else if (lowerSValue.endsWith("mb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * ByteSizeUnit.C2);
            } else if (lowerSValue.endsWith("g")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * ByteSizeUnit.C3);
            } else if (lowerSValue.endsWith("gb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * ByteSizeUnit.C3);
            } else if (lowerSValue.endsWith("t")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * ByteSizeUnit.C4);
            } else if (lowerSValue.endsWith("tb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * ByteSizeUnit.C4);
            } else if (lowerSValue.endsWith("p")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * ByteSizeUnit.C5);
            } else if (lowerSValue.endsWith("pb")) {
                bytes = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)) * ByteSizeUnit.C5);
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
                        "failed to parse setting [{}] with value [{}] as a size in bytes: unit is missing or unrecognized",
                        settingName, sValue);
            }
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("failed to parse [{}]", e, sValue);
        }
        return new ByteSizeValue(bytes, ByteSizeUnit.BYTES);
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
        return Double.hashCode(((double) size) * unit.toBytes(1));
    }

    @Override
    public int compareTo(ByteSizeValue other) {
        double thisValue = ((double) size) * unit.toBytes(1);
        double otherValue = ((double) other.size) * other.unit.toBytes(1);
        return Double.compare(thisValue, otherValue);
    }
}
