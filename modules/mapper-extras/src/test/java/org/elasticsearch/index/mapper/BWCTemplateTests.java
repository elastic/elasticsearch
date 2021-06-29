/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;

/**
 * Rudimentary tests that the templates used by Logstash and Beats
 * prior to their 5.x releases work for newly created indices
 */
public class BWCTemplateTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testBeatsTemplatesBWC() throws Exception {
        byte[] metricBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/metricbeat-6.0.template.json");
        byte[] packetBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/packetbeat-6.0.template.json");
        byte[] fileBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/filebeat-6.0.template.json");
        client().admin().indices().preparePutTemplate("metricbeat").setSource(metricBeat, XContentType.JSON).get();
        client().admin().indices().preparePutTemplate("packetbeat").setSource(packetBeat, XContentType.JSON).get();
        client().admin().indices().preparePutTemplate("filebeat").setSource(fileBeat, XContentType.JSON).get();

        client().prepareIndex("metricbeat-foo").setId("1").setSource("message", "foo").get();
        client().prepareIndex("packetbeat-foo").setId("1").setSource("message", "foo").get();
        client().prepareIndex("filebeat-foo").setId("1").setSource("message", "foo").get();
    }
}
