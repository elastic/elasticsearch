/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.utils;

import org.elasticsearch.xpack.entityanalytics.models.EntityType;

class EntityTypeUtils {
    public static EntityType[] fromCommaSeparatedString(String input) {
        if (input == null || input.isEmpty()) {
            return new EntityType[0];
        }

        String[] parts = input.split(",");
        EntityType[] result = new EntityType[parts.length];

        for (int i = 0; i < parts.length; i++) {
            result[i] = EntityType.valueOf(parts[i].trim());
        }

        return result;
    }
}
