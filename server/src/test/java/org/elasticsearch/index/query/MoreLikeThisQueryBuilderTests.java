/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class MoreLikeThisQueryBuilderTests extends AbstractQueryTestCase<MoreLikeThisQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] { MoreLikeThisQueryBuilder.DOC.getPreferredName() };

    private static String[] randomFields;
    private static Item[] randomLikeItems;
    private static Item[] randomUnlikeItems;

    @Before
    public void setup() {
        // MLT only supports string fields, unsupported fields are tested below
        randomFields = randomStringFields();
        // we also preset the item requests
        randomLikeItems = new Item[randomIntBetween(1, 3)];
        for (int i = 0; i < randomLikeItems.length; i++) {
            randomLikeItems[i] = generateRandomItem();
        }
        // and for the unlike items too
        randomUnlikeItems = new Item[randomIntBetween(1, 3)];
        for (int i = 0; i < randomUnlikeItems.length; i++) {
            randomUnlikeItems[i] = generateRandomItem();
        }
    }

    private static String[] randomStringFields() {
        String[] mappedStringFields = new String[] { TEXT_FIELD_NAME, KEYWORD_FIELD_NAME, TEXT_ALIAS_FIELD_NAME };
        String[] unmappedStringFields = generateRandomStringArray(2, 5, false, false);
        return Stream.concat(Arrays.stream(mappedStringFields), Arrays.stream(unmappedStringFields)).toArray(String[]::new);
    }

    private Item generateRandomItem() {
        String index = randomBoolean() ? getIndex().getName() : null;
        // indexed item or artificial document
        Item item = randomBoolean() ? new Item(index, randomAlphaOfLength(10)) : new Item(index, randomArtificialDoc());

        // if no field is specified MLT uses all mapped fields for this item
        if (randomBoolean()) {
            item.fields(randomFrom(randomFields));
        }
        // per field analyzer
        if (randomBoolean()) {
            item.perFieldAnalyzer(randomPerFieldAnalyzer());
        }
        if (randomBoolean()) {
            item.routing(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            item.version(randomInt(5));
        }
        if (randomBoolean()) {
            item.versionType(randomFrom(VersionType.values()));
        }
        return item;
    }

    private XContentBuilder randomArtificialDoc() {
        XContentBuilder doc;
        try {
            doc = XContentFactory.jsonBuilder().startObject();
            for (String field : randomFields) {
                doc.field(field, randomAlphaOfLength(10));
            }
            doc.endObject();
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to generate random artificial doc!");
        }
        return doc;
    }

    private Map<String, String> randomPerFieldAnalyzer() {
        Map<String, String> perFieldAnalyzer = new HashMap<>();
        for (String field : randomFields) {
            perFieldAnalyzer.put(field, randomAnalyzer());
        }
        return perFieldAnalyzer;
    }

    @Override
    protected MoreLikeThisQueryBuilder doCreateTestQueryBuilder() {
        MoreLikeThisQueryBuilder queryBuilder;
        String[] likeTexts = null;
        Item[] likeItems = null;
        // like field is required
        if (randomBoolean()) {
            likeTexts = generateRandomStringArray(5, 5, false, false);
        } else {
            likeItems = randomLikeItems;
        }
        if (randomBoolean() && CollectionUtils.isEmpty(likeItems) == false) { // for the default field
            queryBuilder = new MoreLikeThisQueryBuilder(null, likeItems);
        } else {
            queryBuilder = new MoreLikeThisQueryBuilder(randomFields, likeTexts, likeItems);
        }

        if (randomBoolean() && queryBuilder.fields() != null) {
            queryBuilder.unlike(generateRandomStringArray(5, 5, false, false));
        }
        if (randomBoolean()) {
            queryBuilder.unlike(randomUnlikeItems);
        }
        if (randomBoolean()) {
            queryBuilder.maxQueryTerms(randomIntBetween(1, 25));
        }
        if (randomBoolean()) {
            queryBuilder.minTermFreq(randomInt(5));
        }
        if (randomBoolean()) {
            queryBuilder.minDocFreq(randomInt(5));
        }
        if (randomBoolean()) {
            queryBuilder.maxDocFreq(randomInt(100));
        }
        if (randomBoolean()) {
            queryBuilder.minWordLength(randomInt(5));
        }
        if (randomBoolean()) {
            queryBuilder.maxWordLength(randomInt(25));
        }
        if (randomBoolean()) {
            queryBuilder.stopWords(generateRandomStringArray(5, 5, false, false));
        }
        if (randomBoolean()) {
            queryBuilder.analyzer(randomAnalyzer());  // fix the analyzer?
        }
        if (randomBoolean()) {
            queryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            queryBuilder.boostTerms(randomFloat() * 10);
        }
        if (randomBoolean()) {
            queryBuilder.include(randomBoolean());
        }
        if (randomBoolean()) {
            queryBuilder.failOnUnsupportedField(randomBoolean());
        }
        return queryBuilder;
    }

    /**
     * we don't want to shuffle the "doc" field internally in {@link #testFromXContent()} because even though the
     * documents would be functionally the same, their {@link BytesReference} representation isn't and thats what we
     * compare when check for equality of the original and the shuffled builder
     */
    @Override
    protected String[] shuffleProtectedFields() {
        return SHUFFLE_PROTECTED_FIELDS;
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // doc contains arbitrary content, anything can be added to it and no exception will be thrown
        return Collections.singletonMap(MoreLikeThisQueryBuilder.DOC.getPreferredName(), null);
    }

    @Override
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        try {
            MultiTermVectorsItemResponse[] responses = new MultiTermVectorsItemResponse[mtvRequest.size()];
            int i = 0;
            for (TermVectorsRequest request : mtvRequest) {
                TermVectorsResponse response = new TermVectorsResponse(request.index(), request.id());
                response.setExists(true);
                Fields generatedFields;
                if (request.doc() != null) {
                    generatedFields = generateFields(randomFields, request.doc().utf8ToString());
                } else {
                    generatedFields = generateFields(
                        request.selectedFields().toArray(new String[request.selectedFields().size()]),
                        request.id()
                    );
                }
                EnumSet<TermVectorsRequest.Flag> flags = EnumSet.of(TermVectorsRequest.Flag.Positions, TermVectorsRequest.Flag.Offsets);
                response.setFields(generatedFields, request.selectedFields(), flags, generatedFields);
                responses[i++] = new MultiTermVectorsItemResponse(response, null);
            }
            return new MultiTermVectorsResponse(responses);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
    }

    /**
     * Here we could go overboard and use a pre-generated indexed random document for a given Item,
     * but for now we'd prefer to simply return the id as the content of the document and that for
     * every field.
     */
    private static Fields generateFields(String[] fieldNames, String text) throws IOException {
        MemoryIndex index = new MemoryIndex();
        for (String fieldName : fieldNames) {
            index.addField(fieldName, text, new WhitespaceAnalyzer());
        }
        return index.createSearcher().getIndexReader().getTermVectors(0);
    }

    @Override
    protected void doAssertLuceneQuery(MoreLikeThisQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        if (CollectionUtils.isEmpty(queryBuilder.likeItems()) == false) {
            assertThat(query, instanceOf(BooleanQuery.class));
            BooleanQuery booleanQuery = (BooleanQuery) query;
            for (BooleanClause booleanClause : booleanQuery) {
                if (booleanClause.getQuery()instanceof MoreLikeThisQuery moreLikeThisQuery) {
                    assertThat(moreLikeThisQuery.getLikeFields().length, greaterThan(0));
                }
            }
        } else {
            // we rely on integration tests for a deeper check here
            assertThat(query, instanceOf(MoreLikeThisQuery.class));
        }
    }

    public void testValidateEmptyFields() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MoreLikeThisQueryBuilder(new String[0], new String[] { "likeText" }, null)
        );
        assertThat(e.getMessage(), containsString("requires 'fields' to be specified"));
    }

    public void testValidateEmptyLike() {
        String[] likeTexts = randomBoolean() ? null : new String[0];
        Item[] likeItems = randomBoolean() ? null : new Item[0];
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MoreLikeThisQueryBuilder(likeTexts, likeItems));
        assertThat(e.getMessage(), containsString("requires either 'like' texts or items to be specified"));
    }

    public void testUnsupportedFields() throws IOException {
        String unsupportedField = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
        MoreLikeThisQueryBuilder queryBuilder = new MoreLikeThisQueryBuilder(
            new String[] { unsupportedField },
            new String[] { "some text" },
            null
        ).failOnUnsupportedField(true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder.toQuery(createSearchExecutionContext())
        );
        assertThat(e.getMessage(), containsString("more_like_this only supports text/keyword fields"));
    }

    public void testUsesIndexAnalyzer() throws IOException {
        MoreLikeThisQueryBuilder qb = new MoreLikeThisQueryBuilder(new String[] { KEYWORD_FIELD_NAME }, new String[] { "some text" }, null);
        MoreLikeThisQuery q = (MoreLikeThisQuery) qb.toQuery(createSearchExecutionContext());
        try (TokenStream ts = q.getAnalyzer().tokenStream(KEYWORD_FIELD_NAME, "some text")) {
            ts.reset();
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            assertTrue(ts.incrementToken());
            assertEquals("some text", termAtt.toString());
            assertFalse(ts.incrementToken());
            ts.end();
        }
    }

    public void testDefaultField() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();

        {
            MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(new String[] { "hello world" }, null);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.toQuery(context));
            assertThat(e.getMessage(), containsString("[more_like_this] query cannot infer"));
        }

        {
            context.getIndexSettings()
                .updateIndexMetadata(
                    newIndexMeta(
                        "index",
                        context.getIndexSettings().getSettings(),
                        Settings.builder().putList("index.query.default_field", TEXT_FIELD_NAME).build()
                    )
                );
            try {
                MoreLikeThisQueryBuilder builder = new MoreLikeThisQueryBuilder(new String[] { "hello world" }, null);
                builder.toQuery(context);
            } finally {
                // Reset the default value
                context.getIndexSettings()
                    .updateIndexMetadata(
                        newIndexMeta(
                            "index",
                            context.getIndexSettings().getSettings(),
                            Settings.builder().putList("index.query.default_field", "*").build()
                        )
                    );
            }
        }
    }

    public void testMoreLikeThisBuilder() throws Exception {
        Query parsedQuery = parseQuery(
            moreLikeThisQuery(new String[] { "name.first", "name.last" }, new String[] { "something" }, null).minTermFreq(1)
                .maxQueryTerms(12)
        ).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    public void testValidateMaxQueryTerms() {
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> new MoreLikeThisQueryBuilder(new String[] { "name.first", "name.last" }, new String[] { "something" }, null)
                .maxQueryTerms(0)
        );
        assertThat(e1.getMessage(), containsString("requires 'maxQueryTerms' to be greater than 0"));

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new MoreLikeThisQueryBuilder(new String[] { "name.first", "name.last" }, new String[] { "something" }, null)
                .maxQueryTerms(-3)
        );
        assertThat(e2.getMessage(), containsString("requires 'maxQueryTerms' to be greater than 0"));
    }

    public void testItemSerialization() throws IOException {
        Item expectedItem = generateRandomItem();
        BytesStreamOutput output = new BytesStreamOutput();
        expectedItem.writeTo(output);
        Item newItem = new Item(output.bytes().streamInput());
        assertEquals(expectedItem, newItem);
    }

    public void testItemCopy() throws IOException {
        Item expectedItem = generateRandomItem();
        Item newItem = new Item(expectedItem);
        assertEquals(expectedItem, newItem);
    }

    public void testItemFromXContent() throws IOException {
        Item expectedItem = generateRandomItem();
        String json = Strings.toString(expectedItem.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        Item newItem = Item.parse(parser, new Item());
        assertEquals(expectedItem, newItem);
    }

    /**
     * Check that this query is generally not cacheable, except when we fetch 0 items
     */
    @Override
    public void testCacheability() throws IOException {
        MoreLikeThisQueryBuilder queryBuilder = createTestQueryBuilder();
        boolean isCacheable = queryBuilder.likeItems().length == 0; // items are always fetched
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertEquals(
            "query should " + (isCacheable ? "" : "not") + " be cacheable: " + queryBuilder.toString(),
            isCacheable,
            context.isCacheable()
        );

        // specifically trigger case where query is cacheable
        queryBuilder = new MoreLikeThisQueryBuilder(randomStringFields(), new String[] { "some text" }, null);
        context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        // specifically trigger case where query is not cacheable
        queryBuilder = new MoreLikeThisQueryBuilder(randomStringFields(), null, new Item[] { new Item("foo", "1") });
        context = createSearchExecutionContext();
        rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "more_like_this" : {
                "fields" : [ "title", "description" ],
                "like" : [ "and potentially some more text here as well", {
                  "_index" : "imdb",
                  "_id" : "1"
                }, {
                  "_index" : "imdb",
                  "_id" : "2"
                } ],
                "max_query_terms" : 12,
                "min_term_freq" : 1,
                "min_doc_freq" : 5,
                "max_doc_freq" : 2147483647,
                "min_word_length" : 0,
                "max_word_length" : 0,
                "minimum_should_match" : "30%",
                "boost_terms" : 0.0,
                "include" : false,
                "fail_on_unsupported_field" : true,
                "boost" : 1.0
              }
            }""";

        MoreLikeThisQueryBuilder parsed = (MoreLikeThisQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, parsed.fields().length);
        assertEquals(json, "and potentially some more text here as well", parsed.likeTexts()[0]);
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(MoreLikeThisQueryBuilder.class));
        return query;
    }

    private static IndexMetadata newIndexMeta(String name, Settings oldIndexSettings, Settings indexSettings) {
        Settings build = Settings.builder().put(oldIndexSettings).put(indexSettings).build();
        return IndexMetadata.builder(name).settings(build).build();
    }
}
