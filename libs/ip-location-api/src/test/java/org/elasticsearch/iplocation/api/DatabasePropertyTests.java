/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class DatabasePropertyTests extends ESTestCase {

    /**
     * Verifies that every {@link DatabaseProperty} enum value has a corresponding method
     * on the {@link IpLocationInfoCollector} interface, ensuring they stay in sync.
     */
    public void testAllDatabasePropertiesHaveCollectorMethods() {
        Set<String> collectorMethodNames = Arrays.stream(IpLocationInfoCollector.class.getMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        for (DatabaseProperty property : DatabaseProperty.values()) {
            String expectedMethod = snakeToCamel(property.fieldName());
            assertTrue(
                "DatabaseProperty."
                    + property.name()
                    + " (field name '"
                    + property.fieldName()
                    + "') has no corresponding method '"
                    + expectedMethod
                    + "' on IpLocationInfoCollector",
                collectorMethodNames.contains(expectedMethod)
            );
        }
    }

    /**
     * Verifies that every method on {@link IpLocationInfoCollector} corresponds to a
     * {@link DatabaseProperty} enum value, ensuring no orphaned methods exist.
     */
    public void testAllCollectorMethodsHaveDatabaseProperties() {
        Set<String> propertyFieldNames = Arrays.stream(DatabaseProperty.values())
            .map(DatabaseProperty::fieldName)
            .collect(Collectors.toSet());

        for (Method method : IpLocationInfoCollector.class.getMethods()) {
            String fieldName = camelToSnake(method.getName());
            assertTrue(
                "IpLocationInfoCollector method '"
                    + method.getName()
                    + "' has no corresponding DatabaseProperty with field name '"
                    + fieldName
                    + "'",
                propertyFieldNames.contains(fieldName)
            );
        }
    }

    private static String snakeToCamel(String snake) {
        StringBuilder sb = new StringBuilder();
        boolean capitalizeNext = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                sb.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String camelToSnake(String camel) {
        StringBuilder sb = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('_');
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
