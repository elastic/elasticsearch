/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.qlcore.index;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.EsField;
import org.elasticsearch.xpack.qlcore.type.UnsupportedEsField;

import java.util.Map;

import static org.elasticsearch.xpack.qlcore.index.VersionCompatibilityChecks.isTypeSupportedInVersion;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.qlcore.type.Types.propagateUnsupportedType;

public final class IndexCompatibility {

    public static Map<String, EsField> compatible(Map<String, EsField> mapping, Version version) {
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

    public static EsIndex compatible(EsIndex esIndex, Version version) {
        compatible(esIndex.mapping(), version);
        return esIndex;
    }

    public static IndexResolution compatible(IndexResolution indexResolution, Version version) {
        if (indexResolution.isValid()) {
            compatible(indexResolution.get(), version);
        }
        return indexResolution;
    }
}
