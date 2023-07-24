/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ExplainActionIT extends ESIntegTestCase {
    public void testSimple() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field", "value1").get();

        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertFalse(response.isExists()); // not a match b/c not realtime
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertFalse(response.isMatch()); // not a match b/c not realtime

        refresh();
        response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));

        response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.termQuery("field", "value2")).get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertNotNull(response.getExplanation());
        assertFalse(response.getExplanation().isMatch());

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field", "value1")).must(QueryBuilders.termQuery("field", "value2"))
            )
            .get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertNotNull(response.getExplanation());
        assertFalse(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getDetails().length, equalTo(2));

        response = client().prepareExplain(indexOrAlias(), "2").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertFalse(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("2"));
    }

    public void testExplainWithFields() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping("obj1.field1", "type=keyword,store=true", "obj1.field2", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
        );
        ensureGreen("test");

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()
            )
            .get();

        refresh();
        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        Set<String> fields = new HashSet<>(response.getGetResult().getFields().keySet());
        assertThat(fields, equalTo(singleton("obj1.field1")));
        assertThat(response.getGetResult().getFields().get("obj1.field1").getValue().toString(), equalTo("value1"));
        assertThat(response.getGetResult().isSourceEmpty(), equalTo(true));

        refresh();
        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1")
            .setFetchSource(true)
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        fields = new HashSet<>(response.getGetResult().getFields().keySet());
        assertThat(fields, equalTo(singleton("obj1.field1")));
        assertThat(response.getGetResult().getFields().get("obj1.field1").getValue().toString(), equalTo("value1"));
        assertThat(response.getGetResult().isSourceEmpty(), equalTo(false));

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1", "obj1.field2")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        String v1 = response.getGetResult().field("obj1.field1").getValue();
        String v2 = response.getGetResult().field("obj1.field2").getValue();
        assertThat(v1, equalTo("value1"));
        assertThat(v2, equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testExplainWithSource() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen("test");

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()
            )
            .get();

        refresh();
        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource("obj1.field1", null)
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        assertThat(response.getGetResult().getSource().size(), equalTo(1));
        assertThat(((Map<String, Object>) response.getGetResult().getSource().get("obj1")).get("field1").toString(), equalTo("value1"));

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource(null, "obj1.field2")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertThat(((Map<String, Object>) response.getGetResult().getSource().get("obj1")).get("field1").toString(), equalTo("value1"));
    }

    public void testExplainWithFilteredAlias() {
        assertAcked(
            prepareCreate("test").setMapping("field2", "type=text")
                .addAlias(new Alias("alias1").filter(QueryBuilders.termQuery("field2", "value2")))
        );
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").get();
        refresh();

        ExplainResponse response = client().prepareExplain("alias1", "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
    }

    public void testExplainWithFilteredAliasFetchSource() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field2", "type=text")
                .addAlias(new Alias("alias1").filter(QueryBuilders.termQuery("field2", "value2")))
        );
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").get();
        refresh();

        ExplainResponse response = client().prepareExplain("alias1", "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource(true)
            .get();

        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getGetResult(), notNullValue());
        assertThat(response.getGetResult().getIndex(), equalTo("test"));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        assertThat(response.getGetResult().getSource(), notNullValue());
        assertThat(response.getGetResult().getSource().get("field1"), equalTo("value1"));
    }

    public void testExplainDateRangeInQueryString() {
        createIndex("test");

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String aMonthAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(now.minusMonths(1));
        String aMonthFromNow = DateTimeFormatter.ISO_LOCAL_DATE.format(now.plusMonths(1));

        client().prepareIndex("test").setId("1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ExplainResponse explainResponse = client().prepareExplain("test", "1").setQuery(queryStringQuery("past:[now-2M/d TO now/d]")).get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.isMatch(), equalTo(true));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    public void testStreamExplain() throws Exception {
        Explanation exp = Explanation.match(2f, "some explanation");

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        Lucene.writeExplanation(out, exp);

        // read
        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);

        Explanation result = Lucene.readExplanation(esBuffer);
        assertThat(exp.toString(), equalTo(result.toString()));

        exp = Explanation.match(2.0f, "some explanation", Explanation.match(2.0f, "another explanation"));

        // write complex
        outBuffer = new ByteArrayOutputStream();
        out = new OutputStreamStreamOutput(outBuffer);
        Lucene.writeExplanation(out, exp);

        // read complex
        esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        esBuffer = new InputStreamStreamInput(esInBuffer);

        result = Lucene.readExplanation(esBuffer);
        assertThat(exp.toString(), equalTo(result.toString()));
    }

    public void testQueryRewrite() {
        indicesAdmin().prepareCreate("twitter")
            .setMapping("user", "type=keyword", "followers", "type=keyword")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2))
            .get();
        ensureGreen("twitter");

        client().prepareIndex("twitter").setId("1").setSource("user", "user1", "followers", new String[] { "user2", "user3" }).get();
        client().prepareIndex("twitter").setId("2").setSource("user", "user2", "followers", new String[] { "user1" }).get();
        refresh();

        TermsQueryBuilder termsLookupQuery = QueryBuilders.termsLookupQuery("user", new TermsLookup("twitter", "2", "followers"));
        ExplainResponse response = client().prepareExplain("twitter", "1").setQuery(termsLookupQuery).execute().actionGet();

        Explanation explanation = response.getExplanation();
        assertNotNull(explanation);
        assertTrue(explanation.isMatch());
    }
}
