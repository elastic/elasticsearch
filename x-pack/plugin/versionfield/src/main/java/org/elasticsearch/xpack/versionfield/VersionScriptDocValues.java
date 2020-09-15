/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.xpack.versionfield.VersionEncoder.VersionParts;

import java.io.IOException;

public final class VersionScriptDocValues extends ScriptDocValues<String> {

    private final SortedSetDocValues in;
    private long[] ords = new long[0];
    private int count;

    public VersionScriptDocValues(SortedSetDocValues in) {
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        count = 0;
        if (in.advanceExact(docId)) {
            for (long ord = in.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = in.nextOrd()) {
                ords = ArrayUtil.grow(ords, count + 1);
                ords[count++] = ord;
            }
        }
    }

    public String getValue() {
        if (count == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! " + "Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
        }
        return get(0);
    }

    @Override
    public String get(int index) {
        try {
            return VersionEncoder.decodeVersion(in.lookupOrd(ords[index]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return count;
    }

    public boolean isValid() {
        return VersionEncoder.legalVersionString(VersionParts.ofVersion(getValue()));
    }

    public boolean isRelease() {
        VersionParts parts = VersionParts.ofVersion(getValue());
        return VersionEncoder.legalVersionString(parts) && parts.preRelease == null;
    }

    public Integer getMajor() {
        VersionParts parts = VersionParts.ofVersion(getValue());
        if (VersionEncoder.legalVersionString(parts) && parts.mainVersion != null) {
            int firstDot = parts.mainVersion.indexOf(".");
            if (firstDot > 0) {
                return Integer.valueOf(parts.mainVersion.substring(0, firstDot));
            } else {
                return Integer.valueOf(parts.mainVersion);
            }
        }
        return null;
    }

    public Integer getMinor() {
        VersionParts parts = VersionParts.ofVersion(getValue());
        Integer rc = null;
        if (VersionEncoder.legalVersionString(parts) && parts.mainVersion != null) {
            int firstDot = parts.mainVersion.indexOf(".");
            if (firstDot > 0) {
                int secondDot = parts.mainVersion.indexOf(".", firstDot + 1);
                if (secondDot > 0) {
                    rc = Integer.valueOf(parts.mainVersion.substring(firstDot + 1, secondDot));
                } else {
                    rc = Integer.valueOf(parts.mainVersion.substring(firstDot + 1));
                }
            }
        }
        return rc;
    }

    public Integer getPatch() {
        VersionParts parts = VersionParts.ofVersion(getValue());
        Integer rc = null;
        if (VersionEncoder.legalVersionString(parts) && parts.mainVersion != null) {
            int firstDot = parts.mainVersion.indexOf(".");
            if (firstDot > 0) {
                int secondDot = parts.mainVersion.indexOf(".", firstDot + 1);
                if (secondDot > 0) {
                    int thirdDot = parts.mainVersion.indexOf(".", secondDot + 1);
                    if (thirdDot > 0) {
                        rc = Integer.valueOf(parts.mainVersion.substring(secondDot + 1, thirdDot));
                    } else {
                        rc = Integer.valueOf(parts.mainVersion.substring(secondDot + 1));
                    }
                }
            }
        }
        return rc;
    }
}
