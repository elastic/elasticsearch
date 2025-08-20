/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class WildcardSearchIT extends ESIntegTestCase {

    private List<String> terms = null;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Wildcard.class);
    }

    @Before
    public void setup() throws IOException {
        terms = new ArrayList<>();
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("wildcard")
            .field("type", "wildcard")
            .endObject()
            .startObject("keyword")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        indicesAdmin().prepareCreate("test").setMapping(xcb).get();
        final int numDocs = randomIntBetween(100, 1000);
        final BulkRequestBuilder builder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            if (rarely()) {
                indexMultiValue(builder);
            } else {
                indexSingleValue(builder);
            }
        }
        assertFalse(builder.get().hasFailures());
        indicesAdmin().prepareRefresh("test").get();
    }

    private void indexSingleValue(BulkRequestBuilder builder) {
        String term = randomIndexString();
        builder.add(
            new IndexRequest("test").source("{\"wildcard\" : \"" + term + "\", \"keyword\" : \"" + term + "\"}", XContentType.JSON)
        );
        terms.add(term);
    }

    private void indexMultiValue(BulkRequestBuilder builder) {
        int docSize = randomIntBetween(1, 10);
        String[] docTerms = new String[docSize];
        for (int i = 0; i < docSize; i++) {
            String term = randomIndexString();
            terms.add(term);
            docTerms[i] = "\"" + term + "\"";
        }
        builder.add(
            new IndexRequest("test").source(
                "{\"wildcard\" : " + Arrays.toString(docTerms) + ", \"keyword\" : " + Arrays.toString(docTerms) + "}",
                XContentType.JSON
            )
        );
    }

    public void testTermQueryDuel() {
        for (int i = 0; i < 50; i++) {
            String term = randomQueryString(terms);
            TermQueryBuilder termQueryBuilder1 = new TermQueryBuilder("wildcard", term);
            TermQueryBuilder termQueryBuilder2 = new TermQueryBuilder("keyword", term);
            assertResponse(
                client().prepareSearch("test").setQuery(termQueryBuilder1),
                response -> assertResponse(
                    client().prepareSearch("test").setQuery(termQueryBuilder2),
                    response2 -> assertThat(
                        response.getHits().getTotalHits().value(),
                        Matchers.equalTo(response2.getHits().getTotalHits().value())
                    )
                )
            );
        }
    }

    public void testTermsQueryDuel() {
        for (int i = 0; i < 10; i++) {
            String[] terms = new String[randomIntBetween(2, 8192)];
            for (int j = 0; j < terms.length; j++) {
                terms[j] = randomQueryString(this.terms);
            }
            TermsQueryBuilder termsQueryBuilder1 = new TermsQueryBuilder("wildcard", terms);
            TermsQueryBuilder termsQueryBuilder2 = new TermsQueryBuilder("keyword", terms);
            assertResponse(
                client().prepareSearch("test").setQuery(termsQueryBuilder1),
                response -> assertResponse(
                    client().prepareSearch("test").setQuery(termsQueryBuilder2),
                    response2 -> assertThat(
                        response.getHits().getTotalHits().value(),
                        Matchers.equalTo(response2.getHits().getTotalHits().value())
                    )
                )
            );
        }
    }

    private static String randomIndexString() {
        String string = randomAlphaOfLength(randomIntBetween(0, 30));
        if (rarely()) {
            return string + "*";
        } else if (rarely()) {
            return "*" + string;
        } else if (rarely()) {
            return "*" + string + "*";
        } else {
            return string;
        }
    }

    private static String randomQueryString(List<String> terms) {
        if (rarely()) {
            return terms.get(randomIntBetween(0, terms.size() - 1));
        } else if (randomBoolean()) {
            return randomAlphaOfLength(randomIntBetween(0, 30));
        } else {
            return randomAlphaOfLength(1) + "*";
        }
    }
}
