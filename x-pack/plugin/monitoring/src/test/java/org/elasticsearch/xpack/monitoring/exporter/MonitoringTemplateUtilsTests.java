/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.LAST_UPDATED_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.OLD_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.indexName;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.pipelineName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MonitoringTemplateUtilsTests extends ESTestCase {

    public void testPipelineName() {
        assertThat(pipelineName("aBc123"), equalTo("xpack_monitoring_aBc123"));
        assertThat(pipelineName(TEMPLATE_VERSION), equalTo("xpack_monitoring_" + TEMPLATE_VERSION));
        assertThat(pipelineName(OLD_TEMPLATE_VERSION), equalTo("xpack_monitoring_" + OLD_TEMPLATE_VERSION));
    }

    public void testEmptyPipeline() throws IOException {
        final String json = Strings.toString(MonitoringTemplateUtils.emptyPipeline(XContentType.JSON));

        // ensure the description contains the API version
        assertThat(json, containsString("Monitoring API version " + MonitoringTemplateUtils.TEMPLATE_VERSION));
        assertThat(json, containsString("\"processors\":[]"));
        assertThat(json, containsString("\"version\":" + LAST_UPDATED_VERSION));
    }

    public void testIndexName() {
        final long timestamp = ZonedDateTime.of(2017, 8, 3, 13, 47, 58,
            0, ZoneOffset.UTC).toInstant().toEpochMilli();

        DateFormatter formatter = DateFormatter.forPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
        assertThat(indexName(formatter, MonitoredSystem.ES, timestamp),
                equalTo(".monitoring-es-" + TEMPLATE_VERSION + "-2017.08.03"));
        assertThat(indexName(formatter, MonitoredSystem.KIBANA, timestamp),
                equalTo(".monitoring-kibana-" + TEMPLATE_VERSION + "-2017.08.03"));
        assertThat(indexName(formatter, MonitoredSystem.LOGSTASH, timestamp),
                equalTo(".monitoring-logstash-" + TEMPLATE_VERSION + "-2017.08.03"));
        assertThat(indexName(formatter, MonitoredSystem.BEATS, timestamp),
                equalTo(".monitoring-beats-" + TEMPLATE_VERSION + "-2017.08.03"));

        formatter = DateFormatter.forPattern("yyyy-dd-MM-HH.mm.ss").withZone(ZoneOffset.UTC);
        assertThat(indexName(formatter, MonitoredSystem.ES, timestamp),
                equalTo(".monitoring-es-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58"));
        assertThat(indexName(formatter, MonitoredSystem.KIBANA, timestamp),
                equalTo(".monitoring-kibana-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58"));
        assertThat(indexName(formatter, MonitoredSystem.LOGSTASH, timestamp),
                equalTo(".monitoring-logstash-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58"));
        assertThat(indexName(formatter, MonitoredSystem.BEATS, timestamp),
                equalTo(".monitoring-beats-" + TEMPLATE_VERSION + "-2017-03-08-13.47.58"));
    }
}
