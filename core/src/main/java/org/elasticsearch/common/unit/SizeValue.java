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

/**
 *
 */
public class SizeValue implements Streamable {

    private long size;

    private SizeUnit sizeUnit;

    private SizeValue() {

    }

    public SizeValue(long singles) {
        this(singles, SizeUnit.SINGLE);
    }

    public SizeValue(long size, SizeUnit sizeUnit) {
        if (size < 0) {
            throw new IllegalArgumentException("size in SizeValue may not be negative");
        }
        this.size = size;
        this.sizeUnit = sizeUnit;
    }

    public long singles() {
        return sizeUnit.toSingles(size);
    }

    public long getSingles() {
        return singles();
    }

    public long kilo() {
        return sizeUnit.toKilo(size);
    }

    public long getKilo() {
        return kilo();
    }

    public long mega() {
        return sizeUnit.toMega(size);
    }

    public long getMega() {
        return mega();
    }

    public long giga() {
        return sizeUnit.toGiga(size);
    }

    public long getGiga() {
        return giga();
    }

    public long tera() {
        return sizeUnit.toTera(size);
    }

    public long getTera() {
        return tera();
    }

    public long peta() {
        return sizeUnit.toPeta(size);
    }

    public long getPeta() {
        return peta();
    }

    public double kiloFrac() {
        return ((double) singles()) / SizeUnit.C1;
    }

    public double getKiloFrac() {
        return kiloFrac();
    }

    public double megaFrac() {
        return ((double) singles()) / SizeUnit.C2;
    }

    public double getMegaFrac() {
        return megaFrac();
    }

    public double gigaFrac() {
        return ((double) singles()) / SizeUnit.C3;
    }

    public double getGigaFrac() {
        return gigaFrac();
    }

    public double teraFrac() {
        return ((double) singles()) / SizeUnit.C4;
    }

    public double getTeraFrac() {
        return teraFrac();
    }

    public double petaFrac() {
        return ((double) singles()) / SizeUnit.C5;
    }

    public double getPetaFrac() {
        return petaFrac();
    }

    @Override
    public String toString() {
        long singles = singles();
        double value = singles;
        String suffix = "";
        if (singles >= SizeUnit.C5) {
            value = petaFrac();
            suffix = "p";
        } else if (singles >= SizeUnit.C4) {
            value = teraFrac();
            suffix = "t";
        } else if (singles >= SizeUnit.C3) {
            value = gigaFrac();
            suffix = "g";
        } else if (singles >= SizeUnit.C2) {
            value = megaFrac();
            suffix = "m";
        } else if (singles >= SizeUnit.C1) {
            value = kiloFrac();
            suffix = "k";
        }

        return Strings.format1Decimals(value, suffix);
    }

    public static SizeValue parseSizeValue(String sValue) throws ElasticsearchParseException {
        return parseSizeValue(sValue, null);
    }

    public static SizeValue parseSizeValue(String sValue, SizeValue defaultValue) throws ElasticsearchParseException {
        if (sValue == null) {
            return defaultValue;
        }
        long singles;
        try {
            if (sValue.endsWith("k") || sValue.endsWith("K")) {
                singles = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * SizeUnit.C1);
            } else if (sValue.endsWith("m") || sValue.endsWith("M")) {
                singles = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * SizeUnit.C2);
            } else if (sValue.endsWith("g") || sValue.endsWith("G")) {
                singles = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * SizeUnit.C3);
            } else if (sValue.endsWith("t") || sValue.endsWith("T")) {
                singles = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * SizeUnit.C4);
            } else if (sValue.endsWith("p") || sValue.endsWith("P")) {
                singles = (long) (Double.parseDouble(sValue.substring(0, sValue.length() - 1)) * SizeUnit.C5);
            } else {
                singles = Long.parseLong(sValue);
            }
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("failed to parse [{}]", e, sValue);
        }
        return new SizeValue(singles, SizeUnit.SINGLE);
    }

    public static SizeValue readSizeValue(StreamInput in) throws IOException {
        SizeValue sizeValue = new SizeValue();
        sizeValue.readFrom(in);
        return sizeValue;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        size = in.readVLong();
        sizeUnit = SizeUnit.SINGLE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(singles());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SizeValue sizeValue = (SizeValue) o;

        if (size != sizeValue.size) return false;
        if (sizeUnit != sizeValue.sizeUnit) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(size);
        result = 31 * result + (sizeUnit != null ? sizeUnit.hashCode() : 0);
        return result;
    }
}
