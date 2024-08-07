/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.Strings;

/**
 * Support negative byte size value.
 */
public class SignedByteSizeValue extends ByteSizeValue {
    private final long size;

    public static final SignedByteSizeValue ZERO = new SignedByteSizeValue(0, ByteSizeUnit.BYTES);
    public static final SignedByteSizeValue ONE = new SignedByteSizeValue(1, ByteSizeUnit.BYTES);
    public static final SignedByteSizeValue MINUS_ONE = new SignedByteSizeValue(-1, ByteSizeUnit.BYTES);

    public SignedByteSizeValue(long size, ByteSizeUnit unit) {
        super(Math.abs(size), unit);
        this.size = size;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getBytes() {
        return getUnit().toBytes(size);
    }

    @Override
    public long getKb() {
        return getUnit().toKB(size);
    }

    @Override
    public long getMb() {
        return getUnit().toMB(size);
    }

    @Override
    public long getGb() {
        return getUnit().toGB(size);
    }

    @Override
    public long getTb() {
        return getUnit().toTB(size);
    }

    @Override
    public long getPb() {
        return getUnit().toPB(size);
    }

    @Override
    public String getStringRep() {
        return size + getUnit().getSuffix();
    }

    public static SignedByteSizeValue ofBytes(long size) {
        if (size == 0) {
            return ZERO;
        }
        if (size == 1) {
            return ONE;
        }
        if (size == -1) {
            return MINUS_ONE;
        }
        return new SignedByteSizeValue(size, ByteSizeUnit.BYTES);
    }

    @Override
    public String toString() {
        long bytes = Math.abs(getBytes());
        double value = getBytes();
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

    public static SignedByteSizeValue add(ByteSizeValue x, ByteSizeValue y) {
        return new SignedByteSizeValue(Math.addExact(x.getBytes(), y.getBytes()), ByteSizeUnit.BYTES);
    }

    public static SignedByteSizeValue subtract(ByteSizeValue x, ByteSizeValue y) {
        return new SignedByteSizeValue(Math.subtractExact(x.getBytes(), y.getBytes()), ByteSizeUnit.BYTES);
    }

    public static ByteSizeValue min(ByteSizeValue x, ByteSizeValue y) {
        return x.compareTo(y) <= 0 ? x : y;
    }
}
