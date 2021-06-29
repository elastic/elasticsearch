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
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.OLD_TEMPLATE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.OLD_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.indexName;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.oldTemplateName;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.pipelineName;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.templateName;
import static org.elasticsearch.xpack.core.template.TemplateUtilsTests.assertTemplate;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class MonitoringTemplateUtilsTests extends ESTestCase {

    public void testTemplateName() {
        assertThat(templateName("abc"), equalTo(".monitoring-abc"));
        assertThat(templateName("XYZ"), equalTo(".monitoring-XYZ"));
        assertThat(templateName("es"), equalTo(".monitoring-es"));
        assertThat(templateName("kibana"), equalTo(".monitoring-kibana"));
        assertThat(templateName("logstash"), equalTo(".monitoring-logstash"));
        assertThat(templateName("beats"), equalTo(".monitoring-beats"));
    }

    public void testOldTemplateName() {
        assertThat(oldTemplateName("abc"), equalTo(".monitoring-abc-" + OLD_TEMPLATE_VERSION));
        assertThat(oldTemplateName("XYZ"), equalTo(".monitoring-XYZ-" + OLD_TEMPLATE_VERSION));
        assertThat(oldTemplateName("es"), equalTo(".monitoring-es-" + OLD_TEMPLATE_VERSION));
        assertThat(oldTemplateName("kibana"), equalTo(".monitoring-kibana-" + OLD_TEMPLATE_VERSION));
        assertThat(oldTemplateName("logstash"), equalTo(".monitoring-logstash-" + OLD_TEMPLATE_VERSION));
        assertThat(oldTemplateName("beats"), equalTo(".monitoring-beats-" + OLD_TEMPLATE_VERSION));
    }

    public void testLoadTemplate() throws IOException {
        String source = MonitoringTemplateUtils.loadTemplate("test");

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(source, equalTo("{\n" +
                "  \"index_patterns\": \".monitoring-data-" + TEMPLATE_VERSION + "\",\n" +
                "  \"mappings\": {\n" +
                "    \"_doc\": {\n" +
                "      \"_meta\": {\n" +
                "        \"template.version\": \"" + TEMPLATE_VERSION + "\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n"));
    }

    public void testCreateEmptyTemplate() throws IOException {
        final String id = randomFrom(OLD_TEMPLATE_IDS);
        final String json = MonitoringTemplateUtils.createEmptyTemplate(id);

        // ensure that the index is created with the proper ID
        assertThat(json, containsString("\".monitoring-" + id + "-" + OLD_TEMPLATE_VERSION + "*\""));
        assertThat(json, containsString("\"version\":" + LAST_UPDATED_VERSION));
    }

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
