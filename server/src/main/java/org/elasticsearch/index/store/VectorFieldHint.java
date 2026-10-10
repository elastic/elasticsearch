/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IOContext;

import java.util.Objects;

/**
 * The field a vectors file holds, for a directory deciding how to open the file from that field's mapping. A file name only
 * says which format wrote it, so the reader or writer opening it says the field. Files holding several fields carry no hint.
 */
public record VectorFieldHint(String field) implements IOContext.FileOpenHint {

    public VectorFieldHint {
        Objects.requireNonNull(field);
    }

    /**
     * Returns the hint for the single field written under {@code segmentSuffix}, or {@code null}
     * when that suffix covers no field or several, or the field infos are not known yet, as when a
     * flush creates its writers.
     */
    public static VectorFieldHint forSuffix(FieldInfos fieldInfos, String segmentSuffix) {
        if (fieldInfos == null) {
            return null;
        }
        String only = null;
        for (FieldInfo fi : fieldInfos) {
            if (fi.hasVectorValues() == false) {
                continue;
            }
            String format = fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
            String suffix = fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
            if (format == null || suffix == null) {
                continue;
            }
            if (segmentSuffix.equals(format + "_" + suffix) == false) {
                continue;
            }
            if (only != null) {
                return null;
            }
            only = fi.name;
        }
        return only == null ? null : new VectorFieldHint(only);
    }
}
