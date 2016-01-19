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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class ByteSizeValue implements Streamable {

    private long size;

    private ByteSizeUnit sizeUnit;

    private ByteSizeValue() {

    }

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit sizeUnit) {
        this.size = size;
        this.sizeUnit = sizeUnit;
    }

    public int bytesAsInt() {
        long bytes = bytes();
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size [" + toString() + "] is bigger than max int");
        }
        return (int) bytes;
    }

    public long bytes() {
        return sizeUnit.toBytes(size);
    }

    public long getBytes() {
        return bytes();
    }

    public long kb() {
        return sizeUnit.toKB(size);
    }

    public long getKb() {
        return kb();
    }

    public long mb() {
        return sizeUnit.toMB(size);
    }

    public long getMb() {
        return mb();
    }

    public long gb() {
        return sizeUnit.toGB(size);
    }

    public long getGb() {
        return gb();
    }

    public long tb() {
        return sizeUnit.toTB(size);
    }

    public long getTb() {
        return tb();
    }

    public long pb() {
        return sizeUnit.toPB(size);
    }

    public long getPb() {
        return pb();
    }

    public double kbFrac() {
        return ((double) bytes()) / ByteSizeUnit.C1;
    }

    public double getKbFrac() {
        return kbFrac();
    }

    public double mbFrac() {
        return ((double) bytes()) / ByteSizeUnit.C2;
    }

    public double getMbFrac() {
        return mbFrac();
    }

    public double gbFrac() {
        return ((double) bytes()) / ByteSizeUnit.C3;
    }

    public double getGbFrac() {
        return gbFrac();
    }

    public double tbFrac() {
        return ((double) bytes()) / ByteSizeUnit.C4;
    }

    public double getTbFrac() {
        return tbFrac();
    }

    public double pbFrac() {
        return ((double) bytes()) / ByteSizeUnit.C5;
    }

    public double getPbFrac() {
        return pbFrac();
    }

    @Override
    public String toString() {
        long bytes = bytes();
        double value = bytes;
        String suffix = "b";
        if (bytes >= ByteSizeUnit.C5) {
            value = pbFrac();
            suffix = "pb";
        } else if (bytes >= ByteSizeUnit.C4) {
            value = tbFrac();
            suffix = "tb";
        } else if (bytes >= ByteSizeUnit.C3) {
            value = gbFrac();
            suffix = "gb";
        } else if (bytes >= ByteSizeUnit.C2) {
            value = mbFrac();
            suffix = "mb";
        } else if (bytes >= ByteSizeUnit.C1) {
            value = kbFrac();
            suffix = "kb";
        }
        return Strings.format1Decimals(value, suffix);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, String settingName) throws ElasticsearchParseException {
        return parseBytesSizeValue(sValue, null, settingName);
    }

    public static ByteSizeValue parseBytesSizeValue(String sValue, ByteSizeValue defaultValue, String settingName) throws ElasticsearchParseException {
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
                throw new ElasticsearchParseException("failed to parse setting [{}] with value [{}] as a size in bytes: unit is missing or unrecognized", settingName, sValue);
            }
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("failed to parse [{}]", e, sValue);
        }
        return new ByteSizeValue(bytes, ByteSizeUnit.BYTES);
    }

    public static ByteSizeValue readBytesSizeValue(StreamInput in) throws IOException {
        ByteSizeValue sizeValue = new ByteSizeValue();
        sizeValue.readFrom(in);
        return sizeValue;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        size = in.readVLong();
        sizeUnit = ByteSizeUnit.BYTES;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bytes());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ByteSizeValue sizeValue = (ByteSizeValue) o;

        return bytes() == sizeValue.bytes();
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(size);
        result = 31 * result + (sizeUnit != null ? sizeUnit.hashCode() : 0);
        return result;
    }
}
