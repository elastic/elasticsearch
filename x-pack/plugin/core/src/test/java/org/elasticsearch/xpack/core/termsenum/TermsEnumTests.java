/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;

public class TermsEnumTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), "false").build();
    }

    public void testTermsEnumIPBasic() throws Exception {
        String indexName = "test";
        createIndex(indexName);

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
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh().get().getStatus());
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").timeout(TimeValue.timeValueMillis(10))
            ).get();
            expectMatches(response, "1.2.3.4", "2.2.2.2", "13.3.3.3", "205.0.1.2", "2001:db8::1:0:0:1");
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").searchAfter("13.3.3.3")
            ).get();
            expectMatches(response, "205.0.1.2", "2001:db8::1:0:0:1");
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").string("2")
            ).get();
            expectMatches(response, "2.2.2.2", "205.0.1.2", "2001:db8::1:0:0:1");
        }
        {
            TermsEnumResponse response = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest(indexName).field("ip_addr").string("20")
            ).get();
            expectMatches(response, "205.0.1.2", "2001:db8::1:0:0:1");
        }
    }

    private static void expectMatches(TermsEnumResponse response, String... expectedTerms) {
        assertTrue("terms response incomplete, maybe due to timeout", response.isComplete());
        List<String> terms = response.getTerms();
        assertEquals(expectedTerms.length, terms.size());
        assertThat(terms, contains(expectedTerms));
    }

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
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh().get().getStatus());

        // test for short random prefixes, max length 7 should at least include some separators but not be too long for short ipv4
        for (int prefixLength = 1; prefixLength < 7; prefixLength++) {
            String randomPrefix = NetworkAddress.format(randomIps[randomIntBetween(0, numDocs - 1)]).substring(0, prefixLength);

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
            int searchAfterPosition = randomIntBetween(0, Math.max(0, terms.size() - 1));
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
