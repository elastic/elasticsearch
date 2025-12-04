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

public class MetricValidatorTests extends ESTestCase {
    public void testMetricNameNotNull() {
        String metricName = "es.somemodule.somemetric.total";
        assertThat(MetricValidator.validateMetricName(metricName), equalTo(metricName));

        expectThrows(NullPointerException.class, () -> MetricValidator.validateMetricName(null));
    }

    public void testMaxMetricNameLength() {
        MetricValidator.validateMetricName(metricNameWithLength(255));

        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName(metricNameWithLength(256)));
    }

    public void testESPrefixAndDotSeparator() {
        MetricValidator.validateMetricName("es.somemodule.somemetric.total");

        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("somemodule.somemetric.total"));
        // verify . is a separator
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es_somemodule_somemetric_total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es_somemodule.somemetric.total"));
    }

    public void testNameElementRegex() {
        MetricValidator.validateMetricName("es.somemodulename0.somemetric.total");
        MetricValidator.validateMetricName("es.some_module_name0.somemetric.total");
        MetricValidator.validateMetricName("es.s.somemetric.total");

        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.someModuleName0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.SomeModuleName.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.0some_module_name0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.some_#_name0.somemetric.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.some-name0.somemetric.total"));
    }

    public void testNameHas3ElementsExcludingSuffix() {
        MetricValidator.validateMetricName("es.group.subgroup.total");

        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es."));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.total"));
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.sth.total"));
    }

    public void testNumberOfElementsLimit() {
        MetricValidator.validateMetricName("es.a2.a3.a4.a5.a6.a7.a8.a9.total");

        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.a2.a3.a4.a5.a6.a7.a8.a9.a10.total"));
    }

    public void testElementLengthLimit() {
        MetricValidator.validateMetricName("es.namespace." + "a".repeat(MetricValidator.METRIC_SEGMENT_MAX_LENGTH) + ".total");

        expectThrows(
            IllegalArgumentException.class,
            () -> MetricValidator.validateMetricName("es.namespace." + "a".repeat(MetricValidator.METRIC_SEGMENT_MAX_LENGTH + 1) + ".total")
        );
    }

    public void testLastElementAllowList() {
        for (String suffix : MetricValidator.METRIC_SUFFIXES) {
            MetricValidator.validateMetricName("es.somemodule.somemetric." + suffix);
        }
        expectThrows(IllegalArgumentException.class, () -> MetricValidator.validateMetricName("es.somemodule.somemetric.some_other_suffix"));
    }

    public void testSkipValidationDueToBWC() {
        MetricValidator.validateMetricName("es.threadpool.searchable_snapshots_cache_fetch_async.total");
        MetricValidator.validateMetricName("es.threadpool.searchable_snapshots_cache_prewarming.total");
        MetricValidator.validateMetricName("es.threadpool.security-crypto.total");
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
            for (int j = 0; j < MetricValidator.METRIC_SEGMENT_MAX_LENGTH - 1 && i < remainingChars; j++) {
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
