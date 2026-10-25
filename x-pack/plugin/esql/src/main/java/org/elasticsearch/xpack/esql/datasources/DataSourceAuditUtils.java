/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataSourceAuditUtils {
    public static Map<String, Object> auditSettings(Map<String, Object> rawSettings, Set<String> auditableSettings) {
        var fields = new HashMap<String, Object>();
        for (var entry : rawSettings.entrySet()) {
            if (auditableSettings.contains(entry.getKey())) {
                if (entry.getValue() instanceof String stringValue) {
                    fields.put("settings." + entry.getKey(), ExternalFailures.redactHttpUrl(stringValue));
                } else {
                    fields.put("settings." + entry.getKey(), entry.getValue());
                }
            } else {
                fields.put("settings." + entry.getKey(), DataSourceSetting.MASK_SENTINEL);
            }
        }
        return fields;
    }
}
