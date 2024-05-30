/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.common;

import org.elasticsearch.xpack.entityanalytics.models.EntityType;

public class EntityTypeUtils {
    public static String getIdentifierFieldForEntityType(EntityType entityType) {
        return (entityType.equals(EntityType.Host)) ? "host.name" : "user.name";
    }

    public static String getAggregationNameForEntityType(EntityType entityType) {
        return (entityType.equals(EntityType.Host)) ? "host" : "user";
    }
}
