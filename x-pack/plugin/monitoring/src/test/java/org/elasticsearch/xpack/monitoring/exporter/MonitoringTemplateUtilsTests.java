/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.indexName;
import static org.hamcrest.Matchers.equalTo;

public class MonitoringTemplateUtilsTests extends ESTestCase {

    public void testIndexName() {
        final long timestamp = ZonedDateTime.of(2017, 8, 3, 13, 47, 58, 0, ZoneOffset.UTC).toInstant().toEpochMilli();

        DateFormatter formatter = DateFormatter.forPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
        assertThat(indexName(formatter, MonitoredSystem.ES, timestamp), equalTo(".monitoring-es-" + TEMPLATE_VERSION + "-2017.08.03"));
        assertThat(
            indexName(formatter, MonitoredSystem.KIBANA, timestamp),
            equalTo(".monitoring-kibana-" + TEMPLATE_VERSION + "-2017.08.03")
        );
        assertThat(
            indexName(formatter, MonitoredSystem.LOGSTASH, timestamp),
            equalTo(".monitoring-logstash-" + TEMPLATE_VERSION + "-2017.08.03")
        );
        assertThat(
            indexName(formatter, MonitoredSystem.BEATS, timestamp),
            equalTo(".monitoring-beats-" + TEMPLATE_VERSION + "-2017.08.03")
        );

        formatter = DateFormatter.forPattern("yyyy-dd-MM-HH.mm.ss").withZone(ZoneOffset.UTC);
        assertThat(
            indexName(formatter, MonitoredSystem.ES, timestamp),
            equalTo(".monitoring-es-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58")
        );
        assertThat(
            indexName(formatter, MonitoredSystem.KIBANA, timestamp),
            equalTo(".monitoring-kibana-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58")
        );
        assertThat(
            indexName(formatter, MonitoredSystem.LOGSTASH, timestamp),
            equalTo(".monitoring-logstash-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58")
        );
        assertThat(
            indexName(formatter, MonitoredSystem.BEATS, timestamp),
            equalTo(".monitoring-beats-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58")
        );
    }
}
