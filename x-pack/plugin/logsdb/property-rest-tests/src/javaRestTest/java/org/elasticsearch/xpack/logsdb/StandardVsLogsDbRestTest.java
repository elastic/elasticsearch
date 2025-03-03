/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class StandardVsLogsDbRestTest extends ESRestTestCase {
    public static OnDemandLocalCluster cluster = OnDemandClusterBuilder.create()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeContainer
    static void beforeContainer() {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();

        cluster.init();
    }

    @AfterContainer
    static void afterContainer() throws IOException {
        cluster.teardown();
        closeClients();
    }

    @BeforeProperty
    void beforeProperty() throws IOException {
        initClient();
    }

    @AfterProperty
    void afterProperty() throws Exception {
        cleanUpCluster();
    }

    @Property(shrinking = ShrinkingMode.FULL)
    public void testStuff(@ForAll("input") TestInput input) throws IOException {
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent());
        mappingXContent.map(input.mapping.mapping());

        createTemplates(mappingXContent);
        createDataStreams();

        deleteDataStreams();
        deleteTemplates();

    }

    @Provide
    Arbitrary<TestInput> input() {
        var template = Template.generate(3, 30);

        return template.map(t -> new TestInput(t, Mapping.generate(t)));
    }

    record TestInput(Template template, Mapping mapping) {}

    private void createTemplates(XContentBuilder mapping) throws IOException {
        final Response createBaselineTemplateResponse = createTemplates(
            "my-datastream-template",
            "my-datastream*",
            Settings.builder(),
            mapping,
            101
        );
        assert createBaselineTemplateResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void createDataStreams() throws IOException {
        final Response craeteDataStreamResponse = client().performRequest(new Request("PUT", "_data_stream/my-datastream"));
        assert craeteDataStreamResponse.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private Response createTemplates(
        final String templateName,
        final String pattern,
        final Settings.Builder settings,
        final XContentBuilder mappings,
        int priority
    ) throws IOException {
        final String template = """
            {
              "index_patterns": [ "%s" ],
              "template": {
                "settings":%s,
                "mappings": %s
              },
              "data_stream": {},
              "priority": %d
            }
            """;
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        final String jsonSettings = settings.build().toString();
        final String jsonMappings = Strings.toString(mappings);
        request.setJsonEntity(Strings.format(template, pattern, jsonSettings, jsonMappings, priority));
        return client().performRequest(request);
    }

    private void deleteDataStreams() throws IOException {
        final Response deleteBaselineDataStream = client().performRequest(new Request("DELETE", "/_data_stream/my-datastream"));
        assert deleteBaselineDataStream.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }

    private void deleteTemplates() throws IOException {
        final Response deleteBaselineTemplate = client().performRequest(new Request("DELETE", "/_index_template/my-datastream-template"));
        assert deleteBaselineTemplate.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
    }
}
