/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.telemetry.apm.internal.MetricNameValidator.MAX_ELEMENT_LENGTH;

public class MetricNameValidatorTests extends ESTestCase {
    MetricNameValidator nameValidator = new MetricNameValidator();

    public void testMaxMetricNameLength() {
        nameValidator.validate(metricNameWithLength(255));

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate(metricNameWithLength(256)));
    }

    public void testESPrefixAndDotSeparator() {
        nameValidator.validate("es.somemodule.somemetric.count");

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("somemodule.somemetric.count"));
        // verify . is a separator
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es_somemodule_somemetric_count"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es_somemodule.somemetric.count"));
    }

    public void testNameElementRegex() {
        nameValidator.validate("es.somemodulename0.somemetric.count");
        nameValidator.validate("es.some_module_name0.somemetric.count");
        nameValidator.validate("es.s.somemetric.count");

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.someModuleName0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.SomeModuleName.somemetric.count"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.0some_module_name0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.some_#_name0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.some-name0.somemetric.count"));
    }

    public void testNameHas3Elements() {
        nameValidator.validate("es.group.count");
        nameValidator.validate("es.group.subgroup.count");

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es"));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es."));
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.sth"));
    }

    public void testNumberOfElementsLimit() {
        nameValidator.validate("es.a2.a3.a4.a5.a6.a7.a8.a9.count");

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.a2.a3.a4.a5.a6.a7.a8.a9.a10.count"));
    }

    public void testElementLengthLimit() {
        nameValidator.validate("es." + "a".repeat(MAX_ELEMENT_LENGTH) + ".count");

        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es." + "a".repeat(MAX_ELEMENT_LENGTH + 1) + ".count"));
    }

    public void testLastElementAllowList() {
        for (String suffix : MetricNameValidator.ALLOWED_SUFFIXES) {
            nameValidator.validate("es.somemodule.somemetric." + suffix);
        }
        expectThrows(IllegalArgumentException.class, () -> nameValidator.validate("es.somemodule.somemetric.some_other_suffix"));
    }

    public static String metricNameWithLength(int length) {
        int prefixAndSuffix = "es.".length() + ".utilization".length();
        assert length > prefixAndSuffix : "length too short";

        var remainingChars = length - prefixAndSuffix;
        StringBuilder metricName = new StringBuilder("es.");
        var i = 0;
        while (i < remainingChars) {
            metricName.append("a");
            i++;
            for (int j = 0; j < MetricNameValidator.MAX_ELEMENT_LENGTH - 1 && i < remainingChars; j++) {
                metricName.append("x");
                i++;
            }
            metricName.append(".");
            i++;

        }
        metricName.append("utilization");
        return metricName.toString();
    }
}
