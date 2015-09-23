/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.shield.MarvelSettingsFilter;
import org.elasticsearch.marvel.shield.SecuredClient;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 *
 */
public class ExportersTests extends ESTestCase {

    private Exporters exporters;
    private Map<String, Exporter.Factory> factories;
    private MarvelSettingsFilter settingsFilter;
    private ClusterService clusterService;
    private NodeSettingsService nodeSettingsService;

    @Before
    public void init() throws Exception {
        factories = new HashMap<>();

        // we always need to have the local exporter as it serves as the default one
        factories.put(LocalExporter.TYPE, new LocalExporter.Factory(mock(SecuredClient.class)));

        settingsFilter = mock(MarvelSettingsFilter.class);
        clusterService = mock(ClusterService.class);
        nodeSettingsService = mock(NodeSettingsService.class);
        exporters = new Exporters(Settings.EMPTY, factories, settingsFilter, clusterService, nodeSettingsService);
    }

    @Test
    public void testInitExporters_Default() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", true));
        factories.put("_type", factory);
        Exporters.InternalExporters internalExporters = exporters.initExporters(Settings.builder()
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(0));
        assertThat(internalExporters.exporters.size(), is(1));
        assertThat(internalExporters.exporters, hasKey("default_" + LocalExporter.TYPE));
        assertThat(internalExporters.exporters.get("default_" + LocalExporter.TYPE), instanceOf(LocalExporter.class));
    }

    @Test
    public void testInitExporters_Single() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", true));
        factories.put("_type", factory);
        Exporters.InternalExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(1));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.type", "_type"));
        assertThat(internalExporters.exporters.size(), is(1));
        assertThat(internalExporters.exporters, hasKey("_name"));
        assertThat(internalExporters.exporters.get("_name"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name").type, is("_type"));
    }

    @Test
    public void testInitExporters_Single_Disabled() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", true));
        factories.put("_type", factory);
        Exporters.InternalExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name.type", "_type")
                .put("_name.enabled", false)
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(2));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.type", "_type"));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name.enabled", "false"));

        // the only configured exporter is disabled... yet we intentionally don't fallback on the default

        assertThat(internalExporters.exporters.size(), is(0));
    }

    @Test(expected = SettingsException.class)
    public void testInitExporters_Single_UnknownType() throws Exception {
        exporters.initExporters(Settings.builder()
                .put("_name.type", "unknown_type")
                .build());
    }

    @Test(expected = SettingsException.class)
    public void testInitExporters_Single_MissingExporterType() throws Exception {
        exporters.initExporters(Settings.builder()
                .put("_name.foo", "bar")
                .build());
    }

    @Test
    public void testInitExporters_Mutliple_SameType() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", false));
        factories.put("_type", factory);
        Exporters.InternalExporters internalExporters = exporters.initExporters(Settings.builder()
                .put("_name0.type", "_type")
                .put("_name1.type", "_type")
                .build());

        assertThat(internalExporters, notNullValue());
        assertThat(internalExporters.settings.getAsMap().size(), is(2));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name0.type", "_type"));
        assertThat(internalExporters.settings.getAsMap(), hasEntry("_name1.type", "_type"));
        assertThat(internalExporters.exporters.size(), is(2));
        assertThat(internalExporters.exporters, hasKey("_name0"));
        assertThat(internalExporters.exporters.get("_name0"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name0").type, is("_type"));
        assertThat(internalExporters.exporters, hasKey("_name1"));
        assertThat(internalExporters.exporters.get("_name1"), instanceOf(TestFactory.TestExporter.class));
        assertThat(internalExporters.exporters.get("_name1").type, is("_type"));
    }

    @Test(expected = SettingsException.class)
    public void testInitExporters_Mutliple_SameType_Singletons() throws Exception {
        Exporter.Factory factory = spy(new TestFactory("_type", true));
        factories.put("_type", factory);
        exporters.initExporters(Settings.builder()
                .put("_name0.type", "_type")
                .put("_name1.type", "_type")
                .build());
    }

    static class TestFactory extends Exporter.Factory<TestFactory.TestExporter> {

        public TestFactory(String type, boolean singleton) {
            super(type, singleton);
        }

        @Override
        public TestExporter create(Exporter.Config config) {
            return new TestExporter(type(), config);
        }

        static class TestExporter extends Exporter {

            public TestExporter(String type, Config config) {
                super(type, config);
            }

            @Override
            public void export(Collection<MarvelDoc> marvelDocs) throws Exception {
            }

            @Override
            public void close() {
            }
        }
    }
}
