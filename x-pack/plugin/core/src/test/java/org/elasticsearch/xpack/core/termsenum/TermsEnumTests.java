/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;

public class TermsEnumTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), "false").build();
    }

    public void testTermsEnumKeywordIndexed() throws Exception {
        checkTermsEnumKeywords("true");
    }

    public void testTermsEnumKeywordDocValuesOnly() throws Exception {
        checkTermsEnumKeywords("false");
    }

    private void checkTermsEnumKeywords(String indexed) throws Exception {
        String indexName = "test_" + indexed;
        // only shard is preferred to increase likelihood of having more than one segment, which goes through a different code path
        int numshards = randomBoolean() ? 1 : randomIntBetween(1, 5);
        createIndex(indexName, Settings.builder().put("index.merge.enabled", false).put("number_of_shards", numshards).build());

        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("keyword")
                    .field("type", "keyword")
                    .field("index", indexed)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        indexAndRefresh(indexName, "1", "keyword", "Apple");
        indexAndRefresh(indexName, "2", "keyword", "Able");
        indexAndRefresh(indexName, "3", "keyword", "Banana");
        indexAndRefresh(indexName, "4", "keyword", "Blue");
        indexAndRefresh(indexName, "5", "keyword", "Cheddar");
        {
            TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, new TermsEnumRequest(indexName).field("keyword")).get();
            List<String> terms = response.getTerms();
            assertEquals(5, terms.size());
            assertThat(terms, contains("Able", "Apple", "Banana", "Blue", "Cheddar"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("keyword").string("A")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("Able", "Apple"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("keyword").string("B").searchAfter("Banana")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(1, terms.size());
            assertThat(terms, contains("Blue"));
        }
    }

    private void indexAndRefresh(String indexName, String id, String field, String value) throws IOException {
        client().prepareIndex(indexName)
            .setId(id)
            .setSource(jsonBuilder().startObject().field(field, value).endObject())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }
}
