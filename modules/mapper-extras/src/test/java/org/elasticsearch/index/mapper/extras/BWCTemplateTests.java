/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

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
        byte[] metricBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/extras/metricbeat-6.0.template.json");
        byte[] packetBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/extras/packetbeat-6.0.template.json");
        byte[] fileBeat = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/extras/filebeat-6.0.template.json");
        indicesAdmin().preparePutTemplate("metricbeat").setSource(metricBeat, XContentType.JSON).get();
        indicesAdmin().preparePutTemplate("packetbeat").setSource(packetBeat, XContentType.JSON).get();
        indicesAdmin().preparePutTemplate("filebeat").setSource(fileBeat, XContentType.JSON).get();

        prepareIndex("metricbeat-foo").setId("1").setSource("message", "foo").get();
        prepareIndex("packetbeat-foo").setId("1").setSource("message", "foo").get();
        prepareIndex("filebeat-foo").setId("1").setSource("message", "foo").get();
    }
}
