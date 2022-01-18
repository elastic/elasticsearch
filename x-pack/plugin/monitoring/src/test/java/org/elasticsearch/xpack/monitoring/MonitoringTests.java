/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.templateName;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MonitoringTests extends ESTestCase {

    public void testIsTypedAPMTemplate() throws IOException {
        String templateWithDocJson = "{\"index_patterns\" : [ \".test-*\" ],\"order\" : 1000,"
            + "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0},"
            + "\"mappings\" : {\"doc\" :"
            + "{\"properties\":{\""
            + randomAlphaOfLength(10)
            + "\":{\"type\":\"text\"},\""
            + randomAlphaOfLength(10)
            + "\":{\"type\":\"keyword\"}}"
            + "}}}";

        String templateWithoutDocJson = "{\"index_patterns\" : [ \".test-*\" ],\"order\" : 1000,"
            + "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0},"
            + "\"mappings\" : "
            + "{\"properties\":{\""
            + randomAlphaOfLength(10)
            + "\":{\"type\":\"text\"},\""
            + randomAlphaOfLength(10)
            + "\":{\"type\":\"keyword\"}}"
            + "}}";

        String templateWithoutMappingsJson = "{\"index_patterns\" : [ \".test-*\" ],\"order\" : 1000,"
            + "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0}"
            + "}";

        {
            // let's test the APM templates get removed as expected
            IndexTemplateMetadata apm68TemplateWithDocMapping = createIndexTemplateMetadata("apm-6.8.20", templateWithDocJson);
            IndexTemplateMetadata apm67TemplateWithDocMapping = createIndexTemplateMetadata("apm-6.7.0", templateWithDocJson);
            IndexTemplateMetadata apm71TemplateWithDocMapping = createIndexTemplateMetadata("apm-7.1.0", templateWithDocJson);
            IndexTemplateMetadata apm68TemplateWithoutDocMapping = createIndexTemplateMetadata("apm-6.8.23", templateWithoutDocJson);
            IndexTemplateMetadata apm71TemplateWithoutDocMapping = createIndexTemplateMetadata("apm-7.1.1", templateWithoutDocJson);
            IndexTemplateMetadata apm68TemplateWithoutMappings = createIndexTemplateMetadata("apm-6.8.22", templateWithoutMappingsJson);

            Map<String, IndexTemplateMetadata> templates = new HashMap<>();
            templates.put("apm-6.7.0", apm67TemplateWithDocMapping);
            templates.put("apm-6.8.20", apm68TemplateWithDocMapping);

            templates.put("apm-6.8.23", apm68TemplateWithoutDocMapping);
            templates.put("apm-6.8.22", apm68TemplateWithoutMappings);
            templates.put("apm-7.1.0", apm71TemplateWithDocMapping);
            templates.put("apm-7.1.1", apm71TemplateWithoutDocMapping);

            templates.entrySet().removeIf(entry -> Monitoring.isMonitoringOrApmTypedTemplate(Collections.emptySet(), entry));

            // only the templates starting with [apm-6.] that have the `doc` mapping type must be deleted
            assertThat(templates.size(), is(4));
            assertThat(templates.get("apm-6.8.22"), is(notNullValue()));
            assertThat(templates.get("apm-6.8.23"), is(notNullValue()));
            assertThat(templates.get("apm-7.1.0"), is(notNullValue()));
            assertThat(templates.get("apm-7.1.1"), is(notNullValue()));
        }

        {
            // let's test the monitoring templates get deleted as expected
            String monitoringWithDocTemplateName = templateName(randomFrom(MonitoringTemplateUtils.TEMPLATE_IDS));
            IndexTemplateMetadata monitoringWithDoc = createIndexTemplateMetadata(monitoringWithDocTemplateName, templateWithDocJson);
            String monitoringWithoutDocTemplateName = templateName(randomFrom(MonitoringTemplateUtils.TEMPLATE_IDS));
            IndexTemplateMetadata monitoringWithoutDoc = createIndexTemplateMetadata(monitoringWithDocTemplateName, templateWithoutDocJson);

            Map<String, IndexTemplateMetadata> templates = new HashMap<>();
            templates.put(monitoringWithDocTemplateName, monitoringWithDoc);
            templates.put(monitoringWithoutDocTemplateName, monitoringWithoutDoc);

            Set<String> monitoringTemplates = new HashSet<>();
            monitoringTemplates.add(monitoringWithDocTemplateName);
            monitoringTemplates.add(monitoringWithoutDocTemplateName);

            templates.entrySet().removeIf(entry -> Monitoring.isMonitoringOrApmTypedTemplate(monitoringTemplates, entry));

            // only the templates starting with [apm-6.] that have the `doc` mapping type must be deleted
            assertThat(templates.size(), is(1));
            assertThat(templates.get(monitoringWithoutDocTemplateName), is(notNullValue()));
        }
    }

    public void testGetMonitoringTemplateNames() {
        Set<String> monitoringTemplateNames = Monitoring.getMonitoringTemplateNames();
        Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
            .forEach(id -> { assertThat(monitoringTemplateNames.contains(MonitoringTemplateUtils.templateName(id)), is(true)); });
    }

    private IndexTemplateMetadata createIndexTemplateMetadata(String templateName, String templateJson) throws IOException {
        BytesReference templateBytes = new BytesArray(templateJson);
        final IndexTemplateMetadata indexTemplateMetadata;
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                templateBytes,
                XContentType.JSON
            )
        ) {
            return IndexTemplateMetadata.Builder.fromXContent(parser, templateName);
        }
    }

}
