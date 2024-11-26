/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MetricNameValidatorTests extends ESTestCase {
    public void testMetricNameNotNull() {
        String metricName = "es.somemodule.somemetric.total";
        assertThat(MetricNameValidator.validate(metricName), equalTo(metricName));

        expectThrows(NullPointerException.class, () -> MetricNameValidator.validate(null));
    }

    public void testMaxMetricNameLength() {
        MetricNameValidator.validate(metricNameWithLength(255));

        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate(metricNameWithLength(256)));
    }

    public void testESPrefixAndDotSeparator() {
        MetricNameValidator.validate("es.somemodule.somemetric.total");

        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("somemodule.somemetric.total"));
        // verify . is a separator
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es_somemodule_somemetric_total"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es_somemodule.somemetric.total"));
    }

    public void testNameElementRegex() {
        MetricNameValidator.validate("es.somemodulename0.somemetric.total");
        MetricNameValidator.validate("es.some_module_name0.somemetric.total");
        MetricNameValidator.validate("es.s.somemetric.total");

        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.someModuleName0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.SomeModuleName.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.0some_module_name0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.some_#_name0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.some-name0.somemetric.total"));
    }

    public void testNameHas3Elements() {
        MetricNameValidator.validate("es.group.total");
        MetricNameValidator.validate("es.group.subgroup.total");

        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es"));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es."));
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.sth"));
    }

    public void testNumberOfElementsLimit() {
        MetricNameValidator.validate("es.a2.a3.a4.a5.a6.a7.a8.a9.total");

        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.a2.a3.a4.a5.a6.a7.a8.a9.a10.total"));
    }

    public void testElementLengthLimit() {
        MetricNameValidator.validate("es." + "a".repeat(MetricNameValidator.MAX_ELEMENT_LENGTH) + ".total");

        expectThrows(
            IllegalArgumentException.class,
            () -> MetricNameValidator.validate("es." + "a".repeat(MetricNameValidator.MAX_ELEMENT_LENGTH + 1) + ".total")
        );
    }

    public void testLastElementAllowList() {
        for (String suffix : MetricNameValidator.ALLOWED_SUFFIXES) {
            MetricNameValidator.validate("es.somemodule.somemetric." + suffix);
        }
        expectThrows(IllegalArgumentException.class, () -> MetricNameValidator.validate("es.somemodule.somemetric.some_other_suffix"));
    }

    public void testSkipValidationDueToBWC() {
        for (String partOfMetricName : MetricNameValidator.SKIP_VALIDATION_METRIC_NAMES_DUE_TO_BWC) {
            MetricNameValidator.validate("es.threadpool." + partOfMetricName + ".total");// fake metric name, but with the part that skips
                                                                                         // validation
        }
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
