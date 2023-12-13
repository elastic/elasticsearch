/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricNameValidator {
    private final Pattern ALLOWED_CHARACTERS = Pattern.compile("[a-z][a-z0-9_]*");

    public void validate(String name) {
        String[] elements = name.split("\\.");
        hasESPrefix(elements, name);
        hasOnlyAllowedCharacters(elements, name);
    }

    private static void hasESPrefix(String[] elements, String name) {
        if (elements[0].equals("es") == false) {
            throw new IllegalArgumentException("Metric name should start with \"es.\" prefix. Name was: " + name);
        }
    }

    private void hasOnlyAllowedCharacters(String[] elements, String name) {
        for (String element : elements) {
            Matcher matcher = ALLOWED_CHARACTERS.matcher(element);
            if (matcher.matches() == false) {
                throw new IllegalArgumentException("Metric name should only use [a-z][a-z0-9_]* characters. " +
                    "Element does not match: \"" + element + "\". " +
                    "Name was: " + name);
            }
        }
    }

}
