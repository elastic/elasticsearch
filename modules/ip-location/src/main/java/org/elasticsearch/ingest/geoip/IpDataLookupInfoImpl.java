/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;

import java.util.LinkedHashMap;
import java.util.SequencedMap;
import java.util.Set;

final class IpDataLookupInfoImpl implements IpDataLookupInfo {

    private final SequencedMap<String, Class<?>> fields;
    private final SequencedMap<String, Class<?>> defaultFields;
    private final String databaseType;

    IpDataLookupInfoImpl(Set<DatabaseProperty> properties, Set<DatabaseProperty> defaultProperties, String databaseType) {
        this.fields = toFieldMap(properties);
        this.defaultFields = toFieldMap(defaultProperties);
        this.databaseType = databaseType;
    }

    private static LinkedHashMap<String, Class<?>> toFieldMap(Set<DatabaseProperty> properties) {
        LinkedHashMap<String, Class<?>> map = new LinkedHashMap<>();
        properties.stream().sorted().forEach(property -> map.put(property.fieldName(), property.fieldType()));
        return map;
    }

    @Override
    public SequencedMap<String, Class<?>> getFields() {
        return fields;
    }

    @Override
    public SequencedMap<String, Class<?>> getDefaultFields() {
        return defaultFields;
    }

    @Override
    public String getDatabaseType() {
        return databaseType;
    }
}
