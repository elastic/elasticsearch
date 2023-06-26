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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

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

        templates.entrySet().removeIf(Monitoring::isTypedAPMTemplate);

        // only the templates starting with [apm-6.] that have the `doc` mapping type must be deleted
        assertThat(templates.size(), is(4));
        assertThat(templates.get("apm-6.8.22"), is(notNullValue()));
        assertThat(templates.get("apm-6.8.23"), is(notNullValue()));
        assertThat(templates.get("apm-7.1.0"), is(notNullValue()));
        assertThat(templates.get("apm-7.1.1"), is(notNullValue()));
    }

    public void testGetMissingMonitoringTemplateIds() {
        {
            String templateJsonWithUpToDateVersion = "{\"index_patterns\" : [ \".test-*\" ],\"version\" : 7170099, \"order\": 1000,"
                + "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0}"
                + "}";
            Map<String, IndexTemplateMetadata> templates = Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS).map(id -> {
                try {
                    String templateName = MonitoringTemplateUtils.templateName(id);
                    return createIndexTemplateMetadata(templateName, templateJsonWithUpToDateVersion);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toMap(IndexTemplateMetadata::getName, Function.identity()));

            assertThat(Monitoring.getMissingMonitoringTemplateIds(templates).size(), is(0));
        }

        {
            String templateJsonWith6xVersion = "{\"index_patterns\" : [ \".test-*\" ],\"version\" : 6070299, \"order\": 1000,"
                + "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0}"
                + "}";
            Map<String, IndexTemplateMetadata> templates = Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS).map(id -> {
                try {
                    String templateName = MonitoringTemplateUtils.templateName(id);
                    return createIndexTemplateMetadata(templateName, templateJsonWith6xVersion);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toMap(IndexTemplateMetadata::getName, Function.identity()));

            // all monitoring templates should be considered as "missing" due to the old version
            List<String> missingTemplates = Monitoring.getMissingMonitoringTemplateIds(templates);
            assertThat(missingTemplates.size(), is(MonitoringTemplateUtils.TEMPLATE_IDS.length));

            Arrays.stream(MonitoringTemplateUtils.TEMPLATE_IDS)
                .forEach(id -> assertThat(missingTemplates.contains(MonitoringTemplateUtils.templateName(id)), is(notNullValue())));
        }
    }

    public void testCreateMonitoringTemplates() {
        {
            // invalid template id doens't throw exception
            List<IndexTemplateMetadata> templates = Monitoring.createMonitoringTemplates(Arrays.asList("kibana123"));
            assertThat(templates.size(), is(0));
        }

        {
            String templateIdToCreate = randomFrom(MonitoringTemplateUtils.TEMPLATE_IDS);
            List<IndexTemplateMetadata> templates = Monitoring.createMonitoringTemplates(Arrays.asList(templateIdToCreate));
            assertThat(templates.size(), is(1));

            String expectedTemplateName = MonitoringTemplateUtils.templateName(templateIdToCreate);
            assertThat(templates.get(0).getName(), is(expectedTemplateName));
        }
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
