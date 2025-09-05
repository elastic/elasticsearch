/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.index;

import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.ql.type.Types.propagateUnsupportedType;
import static org.elasticsearch.xpack.sql.index.VersionCompatibilityChecks.isTypeSupportedInVersion;

public final class IndexCompatibility {

    public static Map<String, EsField> compatible(Map<String, EsField> mapping, SqlVersion version) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            EsField esField = entry.getValue();
            DataType dataType = esField.getDataType();
            if (isPrimitive(dataType) == false) {
                compatible(esField.getProperties(), version);
            } else if (isTypeSupportedInVersion(dataType, version) == false) {
                EsField field = new UnsupportedEsField(entry.getKey(), dataType.name(), null, esField.getProperties());
                entry.setValue(field);
                propagateUnsupportedType(entry.getKey(), dataType.name(), esField.getProperties());
            }
        }
        return mapping;
    }

    public static EsIndex compatible(EsIndex esIndex, SqlVersion version) {
        compatible(esIndex.mapping(), version);
        return esIndex;
    }

    public static IndexResolution compatible(IndexResolution indexResolution, SqlVersion version) {
        if (indexResolution.isValid()) {
            compatible(indexResolution.get(), version);
        }
        return indexResolution;
    }
}
