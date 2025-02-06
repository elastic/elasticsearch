/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Make sure the failures on the data node come back as failures over the wire.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlNodeFailureIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFieldPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
        logger.info("settings {}", settings);
        return settings;
    }

    /**
     * Use a runtime field that fails when loading field values to fail the entire query.
     */
    public void testFailureLoadingFields() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "failing_field").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("fail").setSettings(indexSettings(1, 0)).setMapping(mapping.endObject()).get();

        int docCount = 50;
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int d = 0; d < docCount; d++) {
            docs.add(client().prepareIndex("ok").setSource("foo", d));
        }
        docs.add(client().prepareIndex("fail").setSource("foo", 0));
        indexRandom(true, docs);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run("FROM fail,ok | LIMIT 100").close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }
}
