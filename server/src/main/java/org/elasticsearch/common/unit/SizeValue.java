/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchParseException;

import java.io.IOException;

public class SizeValue implements Writeable, Comparable<SizeValue> {

    private final long size;
    private final SizeUnit sizeUnit;

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

    public SizeValue(StreamInput in) throws IOException {
        size = in.readVLong();
        sizeUnit = SizeUnit.SINGLE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(singles());
    }

    public long singles() {
        return sizeUnit.toSingles(size);
    }

    public long kilo() {
        return sizeUnit.toKilo(size);
    }

    public long mega() {
        return sizeUnit.toMega(size);
    }

    public long giga() {
        return sizeUnit.toGiga(size);
    }

    public long tera() {
        return sizeUnit.toTera(size);
    }

    public long peta() {
        return sizeUnit.toPeta(size);
    }

    public double kiloFrac() {
        return ((double) singles()) / SizeUnit.C1;
    }

    public double megaFrac() {
        return ((double) singles()) / SizeUnit.C2;
    }

    public double gigaFrac() {
        return ((double) singles()) / SizeUnit.C3;
    }

    public double teraFrac() {
        return ((double) singles()) / SizeUnit.C4;
    }

    public double petaFrac() {
        return ((double) singles()) / SizeUnit.C5;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        return compareTo((SizeValue) o) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(((double) size) * sizeUnit.toSingles(1));
    }

    @Override
    public int compareTo(SizeValue other) {
        double thisValue = ((double) size) * sizeUnit.toSingles(1);
        double otherValue = ((double) other.size) * other.sizeUnit.toSingles(1);
        return Double.compare(thisValue, otherValue);
    }
}
