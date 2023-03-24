/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
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
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/94378")
    public void testTermsEnumIPBasic() throws Exception {
        String indexName = "test";
        createIndex(
            indexName,
            Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(50, TimeUnit.MILLISECONDS)).build()
        );

        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("ip_addr")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        client().prepareIndex(indexName).setId("1").setSource(jsonBuilder().startObject().field("ip_addr", "1.2.3.4").endObject()).get();
        client().prepareIndex(indexName).setId("2").setSource(jsonBuilder().startObject().field("ip_addr", "205.0.1.2").endObject()).get();
        client().prepareIndex(indexName).setId("3").setSource(jsonBuilder().startObject().field("ip_addr", "2.2.2.2").endObject()).get();
        client().prepareIndex(indexName)
            .setId("4")
            .setSource(jsonBuilder().startObject().field("ip_addr", "2001:db8::1:0:0:1").endObject())
            .get();
        client().prepareIndex(indexName).setId("5").setSource(jsonBuilder().startObject().field("ip_addr", "13.3.3.3").endObject()).get();
        client().admin().indices().prepareRefresh().get();

        {
            TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, new TermsEnumRequest(indexName).field("ip_addr")).get();
            List<String> terms = response.getTerms();
            assertEquals(5, terms.size());
            assertThat(terms, contains("1.2.3.4", "2.2.2.2", "13.3.3.3", "205.0.1.2", "2001:db8::1:0:0:1"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").searchAfter("13.3.3.3")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("205.0.1.2", "2001:db8::1:0:0:1"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").string("2")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(3, terms.size());
            assertThat(terms, contains("2.2.2.2", "205.0.1.2", "2001:db8::1:0:0:1"));
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").string("20")
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(2, terms.size());
            assertThat(terms, contains("205.0.1.2", "2001:db8::1:0:0:1"));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/94382")
    public void testTermsEnumIPRandomized() throws Exception {
        String indexName = "test_random";
        createIndex(indexName);
        int numDocs = 500;

        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("ip_addr")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        // create random ip test data
        InetAddress[] randomIps = new InetAddress[numDocs];
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk(indexName);
        for (int i = 0; i < numDocs; i++) {
            randomIps[i] = randomIp(randomBoolean());
            bulkRequestBuilder.add(
                client().prepareIndex(indexName)
                    .setSource(jsonBuilder().startObject().field("ip_addr", NetworkAddress.format(randomIps[i])).endObject())
            );
        }
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        // test for short random prefixes, max length 7 should at least include some separators but not be too long for short ipv4
        for (int prefixLength = 1; prefixLength < 7; prefixLength++) {
            String randomPrefix = NetworkAddress.format(randomIps[randomIntBetween(0, numDocs)])
                .substring(0, prefixLength)
                .replaceAll("^0*", "");
            int expectedResults = 0;
            for (int i = 0; i < numDocs; i++) {
                if (NetworkAddress.format(randomIps[i]).startsWith(randomPrefix)) {
                    expectedResults++;
                }
            }
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").string(randomPrefix).size(numDocs)
            ).get();
            List<String> terms = response.getTerms();
            assertEquals(
                "expected " + expectedResults + " for prefix " + randomPrefix + " but was " + terms.size() + ", " + terms,
                expectedResults,
                terms.size()
            );

            // test search after functionality
            int searchAfterPosition = randomIntBetween(0, terms.size() - 1);
            expectedResults = expectedResults - searchAfterPosition - 1;
            response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr")
                    .string(randomPrefix)
                    .size(numDocs)
                    .searchAfter(terms.get(searchAfterPosition))
            ).get();
            assertEquals(
                "expected " + expectedResults + " for prefix " + randomPrefix + " but was " + response.getTerms().size() + ", " + terms,
                expectedResults,
                response.getTerms().size()
            );
        }
    }
}
