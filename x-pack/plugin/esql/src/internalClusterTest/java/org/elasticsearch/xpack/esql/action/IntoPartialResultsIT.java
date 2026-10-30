/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * A query that would otherwise answer with a partial result must not write one: {@code INTO} forces every shard to
 * succeed, whatever the request asked for. The failing shard is a runtime field that throws when loaded, read one
 * shard at a time so that the other shard's rows are a partial result rather than a total failure.
 */
public class IntoPartialResultsIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFieldPlugin.class);
        return plugins;
    }

    public void testPartialResultIsRefusedEvenWhenAllowed() throws Exception {
        assumeTrue("INTO is behind the esql_into feature flag", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
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
        assertAcked(indicesAdmin().prepareCreate("fail").setMapping(mapping.endObject()));
        assertAcked(indicesAdmin().prepareCreate("ok").setMapping("foo", "type=long"));
        assertAcked(indicesAdmin().prepareCreate("dst").setMapping("foo", "type=long", "fail_me", "type=long"));
        List<org.elasticsearch.action.index.IndexRequestBuilder> docs = new ArrayList<>();
        for (int d = 0; d < between(1, 20); d++) {
            docs.add(client().prepareIndex("fail").setSource("foo", d));
            docs.add(client().prepareIndex("ok").setSource("foo", d));
        }
        indexRandom(true, docs);

        // Without INTO this request answers with the "ok" rows and a partial flag.
        EsqlQueryRequest reading = request("FROM fail,ok | KEEP foo, fail_me | LIMIT 100");
        try (EsqlQueryResponse response = run(reading)) {
            assertTrue(response.isPartial());
        }

        // With INTO the same request fails instead, so the destination never holds part of an answer.
        EsqlQueryRequest writing = request("FROM fail,ok | KEEP foo, fail_me | INTO dst");
        Exception e = expectThrows(Exception.class, () -> run(writing).close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    private EsqlQueryRequest request(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.allowPartialResults(true);
        // One shard at a time, so the failing shard does not take the whole query down with it.
        request.pragmas(
            new QueryPragmas(
                Settings.builder().put(randomPragmas().getSettings()).put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()
            )
        );
        request.acceptedPragmaRisks(true);
        return request;
    }
}
