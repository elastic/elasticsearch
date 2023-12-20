/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetricNameValidator {
    private static final Pattern ALLOWED_CHARACTERS = Pattern.compile("[a-z][a-z0-9_]*");
    static final Set<String> ALLOWED_SUFFIXES = Set.of(
        "total",
        "current",
        "ratio",
        "status" /*a workaround for enums */,
        "usage",
        "size",
        "utilization",
        "histogram",
        "time"
    );
    static final int MAX_METRIC_NAME_LENGTH = 255;

    static final int MAX_ELEMENT_LENGTH = 30;
    static final int MAX_NUMBER_OF_ELEMENTS = 10;

    private MetricNameValidator() {}

    /**
     * Validates a metric name as per guidelines in Naming.md
     *
     * @param metricName metric name to be validated
     * @throws IllegalArgumentException an exception indicating an incorrect metric name
     */
    public static String validate(String metricName) {
        Objects.requireNonNull(metricName);
        validateMaxMetricNameLength(metricName);

        String[] elements = metricName.split("\\.");
        hasESPrefix(elements, metricName);
        hasAtLeast3Elements(elements, metricName);
        hasNotBreachNumberOfElementsLimit(elements, metricName);
        lastElementIsFromAllowList(elements, metricName);
        perElementValidations(elements, metricName);
        return metricName;
    }

    private static void validateMaxMetricNameLength(String metricName) {
        if (metricName.length() > MAX_METRIC_NAME_LENGTH) {
            throw new IllegalArgumentException(
                "Metric name length "
                    + metricName.length()
                    + "is longer than max metric name length:"
                    + MAX_METRIC_NAME_LENGTH
                    + " Name was: "
                    + metricName
            );
        }
    }

    private static void lastElementIsFromAllowList(String[] elements, String name) {
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

    private static void hasNotBreachNumberOfElementsLimit(String[] elements, String name) {
        if (elements.length > MAX_NUMBER_OF_ELEMENTS) {
            throw new IllegalArgumentException(
                "Metric name should have at most 10 elements. It had: " + elements.length + ". The name was: " + name
            );
        }
    }

    private static void hasAtLeast3Elements(String[] elements, String name) {
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

    private static void perElementValidations(String[] elements, String name) {
        for (String element : elements) {
            hasOnlyAllowedCharacters(element, name);
            hasNotBreachLengthLimit(element, name);
        }
    }

    private static void hasNotBreachLengthLimit(String element, String name) {
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

    private static void hasOnlyAllowedCharacters(String element, String name) {
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
