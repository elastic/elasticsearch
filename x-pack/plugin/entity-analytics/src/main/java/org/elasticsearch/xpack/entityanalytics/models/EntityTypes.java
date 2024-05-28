/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.entityanalytics.models;

public class EntityTypes {
    public static EntityType[] fromStringArray(String[] entityTypeStrings) {
        EntityType[] entityTypes = new EntityType[entityTypeStrings.length];
        for (int i = 0; i < entityTypeStrings.length; i++) {
            var entityTypeString = entityTypeStrings[i].trim().toLowerCase();
            if (entityTypeString.equals("host")) {
                entityTypes[i] = EntityType.Host;
            } else if (entityTypeString.equals("user")) {
                entityTypes[i] = EntityType.User;
            } else {
                throw new IllegalArgumentException("Unknown entity type [" + entityTypeString + "]");
            }
        }

        return entityTypes;
    }
}
