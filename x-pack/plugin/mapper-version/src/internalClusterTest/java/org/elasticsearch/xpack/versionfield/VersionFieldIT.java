/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;

public class VersionFieldIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), "false").build();
    }

    public void testTermsAggregation() throws Exception {
        String indexName = "test";
        createIndex(indexName);

        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("version")
                    .field("type", "version")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "3.11.5").endObject()).get();
        indicesAdmin().prepareRefresh().get();

        // terms aggs
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(AggregationBuilders.terms("myterms").field("version"))
            .get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();

        assertEquals(5, buckets.size());
        assertEquals("1.0", buckets.get(0).getKey());
        assertEquals("1.3.0", buckets.get(1).getKey());
        assertEquals("2.1.0-alpha", buckets.get(2).getKey());
        assertEquals("2.1.0", buckets.get(3).getKey());
        assertEquals("3.11.5", buckets.get(4).getKey());
    }

    public void testTermsEnum() throws Exception {
        String indexName = "test";
        createIndex(indexName);

        indicesAdmin().preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("version")
                    .field("type", "version")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("version", "1.0").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject()).get();
        client().prepareIndex(indexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName).setId("4").setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject()).get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("version", "3.11.5").endObject()).get();
        indicesAdmin().prepareRefresh().get();

        {
            TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, new TermsEnumRequest(indexName).field("version")).get();
            List<String> terms = response.getTerms();
            assertEquals(5, terms.size());
            assertThat(terms, contains("1.0", "1.3.0", "2.1.0-alpha", "2.1.0", "3.11.5"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").searchAfter("2.1.0-alpha")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("2.1.0", "3.11.5"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").string("1.")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("1.0", "1.3.0"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").string("2.1.0")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("2.1.0-alpha", "2.1.0"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").string("2.1.0-")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(1, terms.size());
            assertThat(terms, contains("2.1.0-alpha"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").string("2.1.0-A").caseInsensitive(true)
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(1, terms.size());
            assertThat(terms, contains("2.1.0-alpha"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("version").string("3.1")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(1, terms.size());
            assertThat(terms, contains("3.11.5"));
        }
    }
}
