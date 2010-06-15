/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class SizeValue implements Serializable, Streamable {

    private long size;

    private SizeUnit sizeUnit;

    private SizeValue() {

    }

    public SizeValue(long bytes) {
        this(bytes, SizeUnit.BYTES);
    }

    public SizeValue(long size, SizeUnit sizeUnit) {
        this.size = size;
        this.sizeUnit = sizeUnit;
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

    public double kbFrac() {
        return ((double) bytes()) / SizeUnit.C1;
    }

    public double getKbFrac() {
        return kbFrac();
    }

    public double mbFrac() {
        return ((double) bytes()) / SizeUnit.C2;
    }

    public double getMbFrac() {
        return mbFrac();
    }

    public double gbFrac() {
        return ((double) bytes()) / SizeUnit.C3;
    }

    public double getGbFrac() {
        return gbFrac();
    }

    @Override public String toString() {
        long bytes = bytes();
        double value = bytes;
        String suffix = "";
        if (bytes >= SizeUnit.C3) {
            value = gbFrac();
            suffix = "g";
        } else if (bytes >= SizeUnit.C2) {
            value = mbFrac();
            suffix = "m";
        } else if (bytes >= SizeUnit.C1) {
            value = kbFrac();
            suffix = "k";
        }
        return Strings.format1Decimals(value, suffix);
    }

    public static SizeValue parseSizeValue(String sValue, SizeValue defaultValue) throws ElasticSearchParseException {
        if (sValue == null) {
            return defaultValue;
        }
        long bytes;
        try {
            if (sValue.endsWith("b")) {
                bytes = Long.parseLong(sValue.substring(0, sValue.length() - 1));
            } else if (sValue.endsWith("k") || sValue.endsWith("K")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * 1024);
            } else if (sValue.endsWith("kb")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 2)) * 1024);
            } else if (sValue.endsWith("m") || sValue.endsWith("M")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * 1024 * 1024);
            } else if (sValue.endsWith("mb")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 2)) * 1024 * 1024);
            } else if (sValue.endsWith("g") || sValue.endsWith("G")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * 1024 * 1024 * 1024);
            } else if (sValue.endsWith("gb")) {
                bytes = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 2)) * 1024 * 1024 * 1024);
            } else {
                bytes = Long.parseLong(sValue);
            }
        } catch (NumberFormatException e) {
            throw new ElasticSearchParseException("Failed to parse [" + sValue + "]", e);
        }
        return new SizeValue(bytes, SizeUnit.BYTES);
    }

    public static SizeValue readSizeValue(StreamInput in) throws IOException {
        SizeValue sizeValue = new SizeValue();
        sizeValue.readFrom(in);
        return sizeValue;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        size = in.readVLong();
        sizeUnit = SizeUnit.BYTES;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bytes());
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SizeValue sizeValue = (SizeValue) o;

        if (size != sizeValue.size) return false;
        if (sizeUnit != sizeValue.sizeUnit) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = (int) (size ^ (size >>> 32));
        result = 31 * result + (sizeUnit != null ? sizeUnit.hashCode() : 0);
        return result;
    }
}