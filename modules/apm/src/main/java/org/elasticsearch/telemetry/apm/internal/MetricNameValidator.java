/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetricNameValidator {
    private final Pattern ALLOWED_CHARACTERS = Pattern.compile("[a-z][a-z0-9_]*");
    private final Set<String> ALLOWED_SUFFIXES = Set.of(
        "size",
        "total",
        "count",
        "usage",
        "utilization",
        "histogram",
        "ratio",
        "status" /*a workaround for enums */,
        "time"
    );

    public static final int MAX_ELEMENT_LENGTH = 30;
    public static final int MAX_NUMBER_OF_ELEMENTS = 10;

    public void validate(String name) {
        String[] elements = name.split("\\.");
        hasESPrefix(elements, name);
        hasAtLeast3Elements(elements, name);
        hasNotBreachNumberOfElementsLimit(elements, name);
        lastElementIsFromAllowListOrPlural(elements, name);

        perElementValidations(elements, name);
    }

    private void lastElementIsFromAllowListOrPlural(String[] elements, String name) {
        String lastElement = elements[elements.length - 1];
        if (ALLOWED_SUFFIXES.contains(lastElement) == false) {
            throw new IllegalArgumentException(
                "Metric name should end with one of ["
                    + ALLOWED_SUFFIXES.stream().collect(Collectors.joining(","))
                    + "] "
                    + "Last element was: "
                    + lastElement
                    + ". "
                    + "Name was: "
                    + name
            );
        }
    }

    private void hasNotBreachNumberOfElementsLimit(String[] elements, String name) {
        if (elements.length > MAX_NUMBER_OF_ELEMENTS) {
            throw new IllegalArgumentException(
                "Metric name should have at most 10 elements. It had: " + elements.length + ". The name was: " + name
            );
        }
    }

    private void hasAtLeast3Elements(String[] elements, String name) {
        if (elements.length < 3) {
            throw new IllegalArgumentException(
                "Metric name consist of at least 3 elements. An es. prefix, group and a name. The name was: " + name
            );
        }
    }

    private static void hasESPrefix(String[] elements, String name) {
        if (elements[0].equals("es") == false) {
            throw new IllegalArgumentException(
                "Metric name should start with \"es.\" prefix and use \".\" as a separator. Name was: " + name
            );
        }
    }

    private void perElementValidations(String[] elements, String name) {
        for (String element : elements) {
            hasOnlyAllowedCharacters(element, name);
            hasNotBreachLengthLimit(element, name);
        }
    }

    private void hasNotBreachLengthLimit(String element, String name) {
        if (element.length() > MAX_ELEMENT_LENGTH) {
            throw new IllegalArgumentException(
                "Metric name's element should not be longer than "
                    + MAX_ELEMENT_LENGTH
                    + " characters. Was: "
                    + element.length()
                    + ". Name was: "
                    + name
            );
        }
    }

    private void hasOnlyAllowedCharacters(String element, String name) {
        Matcher matcher = ALLOWED_CHARACTERS.matcher(element);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(
                "Metric name should only use [a-z][a-z0-9_]* characters. "
                    + "Element does not match: \""
                    + element
                    + "\". "
                    + "Name was: "
                    + name
            );
        }
    }

}
