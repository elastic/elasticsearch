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
    private final String databaseType;

    IpDataLookupInfoImpl(Set<DatabaseProperty> properties, String databaseType) {
        LinkedHashMap<String, Class<?>> map = new LinkedHashMap<>();
        for (DatabaseProperty property : properties) {
            map.put(property.fieldName(), property.fieldType());
        }
        this.fields = map;
        this.databaseType = databaseType;
    }

    @Override
    public SequencedMap<String, Class<?>> getFields() {
        return fields;
    }

    @Override
    public String getDatabaseType() {
        return databaseType;
    }
}
