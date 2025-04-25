/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class RrfWithInvalidLicenseIT extends AbstractEsqlIntegTestCase {
    private static final String LICENSE_ERROR_MESSAGE = "current license is non-compliant for [RRF]";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPluginWithNonEnterpriseOrExpiredLicense.class);
    }

    @Before
    public void setupIndex() {
        assumeTrue("requires RRF capability", EsqlCapabilities.Cap.RRF.isEnabled());
        var indexName = "test";
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(new IndexRequestBuilder(client, indexName).setId("1").setSource("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequestBuilder(client, indexName).setId("2").setSource("id", 2, "content", "This is a brown dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }

    public void testRrf() {
        var query = """
            FROM test METADATA _score, _id, _index
            | FORK
               ( WHERE content:"fox" )
               ( WHERE content:"dog" )
            | RRF
            """;

        ElasticsearchException e = expectThrows(VerificationException.class, () -> run(query));

        assertThat(e.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }
}
