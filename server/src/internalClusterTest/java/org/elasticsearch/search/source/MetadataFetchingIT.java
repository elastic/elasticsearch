/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.source;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MetadataFetchingIT extends ESIntegTestCase {
    public void testSimple() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();

        assertResponse(prepareSearch("test").storedFields("_none_").setFetchSource(false).setVersion(true), response -> {
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
            assertThat(response.getHits().getAt(0).getVersion(), notNullValue());
        });

        assertResponse(prepareSearch("test").storedFields("_none_"), response -> {
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        });
    }

    public void testInnerHits() {
        assertAcked(prepareCreate("test").setMapping("nested", "type=nested"));
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "value", "nested", Collections.singletonMap("title", "foo")).get();
        refresh();

        assertResponse(
            prepareSearch("test").storedFields("_none_")
                .setFetchSource(false)
                .setQuery(
                    new NestedQueryBuilder("nested", new TermQueryBuilder("nested.title", "foo"), ScoreMode.Total).innerHit(
                        new InnerHitBuilder().setStoredFieldNames(Collections.singletonList("_none_"))
                            .setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
                    )
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), nullValue());
                assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits hits = response.getHits().getAt(0).getInnerHits().get("nested");
                assertThat(hits.getTotalHits().value(), equalTo(1L));
                assertThat(hits.getAt(0).getId(), nullValue());
                assertThat(hits.getAt(0).getSourceAsString(), nullValue());
            }
        );
    }

    public void testWithRouting() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", "value").setRouting("toto").get();
        refresh();

        assertResponse(prepareSearch("test"), response -> {
            assertThat(response.getHits().getAt(0).getId(), notNullValue());
            assertThat(response.getHits().getAt(0).field("_routing"), notNullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        });
        assertResponse(prepareSearch("test").storedFields("_none_").setFetchSource(false), response -> {
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).field("_routing"), nullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        });
        assertResponse(prepareSearch("test").storedFields("_none_"), response -> {
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        });

        GetResponse getResponse = client().prepareGet("test", "1").setRouting("toto").get();
        assertTrue(getResponse.isExists());
        assertEquals("toto", getResponse.getFields().get("_routing").getValue());
    }

    public void testWithIgnored() {
        assertAcked(prepareCreate("test").setMapping("ip", "type=ip,ignore_malformed=true"));
        ensureGreen();

        prepareIndex("test").setId("1").setSource("ip", "value").get();
        refresh();

        assertResponse(prepareSearch("test"), response -> {
            assertThat(response.getHits().getAt(0).getId(), notNullValue());
            assertThat(response.getHits().getAt(0).field("_ignored").getValue(), equalTo("ip"));
            assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        });
        assertResponse(prepareSearch("test").storedFields("_none_"), response -> {
            assertThat(response.getHits().getAt(0).getId(), nullValue());
            assertThat(response.getHits().getAt(0).field("_ignored"), nullValue());
            assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        });

        {
            GetResponse getResponse = client().prepareGet("test", "1").get();
            assertTrue(getResponse.isExists());
            assertThat(getResponse.getField("_ignored"), nullValue());
        }
        {
            GetResponse getResponse = client().prepareGet("test", "1").setStoredFields("_ignored").get();
            assertTrue(getResponse.isExists());
            assertEquals("ip", getResponse.getField("_ignored").getValue());
        }
    }

    public void testInvalid() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        indexDoc("test", "1", "field", "value");
        refresh();

        {
            ValidationException exc = expectThrows(
                ValidationException.class,
                prepareSearch("test").setFetchSource(true).storedFields("_none_")
            );
            assertThat(exc.getMessage(), containsString("[stored_fields] cannot be disabled if [_source] is requested"));
        }
        {
            ValidationException exc = expectThrows(
                ValidationException.class,
                prepareSearch("test").storedFields("_none_").addFetchField("field")
            );
            assertThat(exc.getMessage(), containsString("[stored_fields] cannot be disabled when using the [fields] option"));
        }
        {
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> prepareSearch("test").storedFields("_none_", "field1")
            );
            assertThat(exc.getMessage(), equalTo("cannot combine _none_ with other fields"));
        }
        {
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> prepareSearch("test").storedFields("_none_").storedFields("field1")
            );
            assertThat(exc.getMessage(), equalTo("cannot combine _none_ with other fields"));
        }
    }

    public void testFetchId() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();

        assertResponse(prepareSearch("test").addFetchField("_id"), response -> {
            assertEquals(1, response.getHits().getHits().length);
            assertEquals("1", response.getHits().getAt(0).getId());
            assertEquals("1", response.getHits().getAt(0).field("_id").getValue());
        });
    }
}
