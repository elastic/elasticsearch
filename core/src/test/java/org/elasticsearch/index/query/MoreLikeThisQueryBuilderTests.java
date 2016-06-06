/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MoreLikeThisQueryBuilderTests extends AbstractQueryTestCase<MoreLikeThisQueryBuilder> {

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
        String[] mappedStringFields = new String[]{STRING_FIELD_NAME, STRING_FIELD_NAME_2};
        String[] unmappedStringFields = generateRandomStringArray(2, 5, false, false);
        return Stream.concat(Arrays.stream(mappedStringFields), Arrays.stream(unmappedStringFields)).toArray(String[]::new);
    }

    private Item generateRandomItem() {
        String index = randomBoolean() ? getIndex().getName() : null;
        String type = getRandomType();  // set to one type to avoid ambiguous types
        // indexed item or artificial document
        Item item;
        if (randomBoolean()) {
            item = new Item(index, type, randomAsciiOfLength(10));
        } else {
            item = new Item(index, type, randomArtificialDoc());
        }
        // if no field is specified MLT uses all mapped fields for this item
        if (randomBoolean()) {
            item.fields(randomFrom(randomFields));
        }
        // per field analyzer
        if (randomBoolean()) {
            item.perFieldAnalyzer(randomPerFieldAnalyzer());
        }
        if (randomBoolean()) {
            item.routing(randomAsciiOfLength(10));
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
                doc.field(field, randomAsciiOfLength(10));
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
        if (randomBoolean()) { // for the default field
            queryBuilder = new MoreLikeThisQueryBuilder(likeTexts, likeItems);
        } else {
            queryBuilder = new MoreLikeThisQueryBuilder(randomFields, likeTexts, likeItems);
        }

        if (randomBoolean()) {
            queryBuilder.unlike(generateRandomStringArray(5, 5, false, false));
        }
        if (randomBoolean()) {
            queryBuilder.unlike(randomUnlikeItems);
        }
        if (randomBoolean()) {
            queryBuilder.maxQueryTerms(randomInt(25));
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

    @Override
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        try {
            MultiTermVectorsItemResponse[] responses = new MultiTermVectorsItemResponse[mtvRequest.size()];
            int i = 0;
            for (TermVectorsRequest request : mtvRequest) {
                TermVectorsResponse response = new TermVectorsResponse(request.index(), request.type(), request.id());
                response.setExists(true);
                Fields generatedFields;
                if (request.doc() != null) {
                    generatedFields = generateFields(randomFields, request.doc().toUtf8());
                } else {
                    generatedFields = generateFields(request.selectedFields().toArray(new String[request.selectedFields().size()]), request.id());
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
        return MultiFields.getFields(index.createSearcher().getIndexReader());
    }

    @Override
    protected void doAssertLuceneQuery(MoreLikeThisQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.likeItems() != null && queryBuilder.likeItems().length > 0) {
            assertThat(query, instanceOf(BooleanQuery.class));
        } else {
            // we rely on integration tests for a deeper check here
            assertThat(query, instanceOf(MoreLikeThisQuery.class));
        }
    }

    public void testValidateEmptyFields() {
        try {
            new MoreLikeThisQueryBuilder(new String[0], new String[]{"likeText"}, null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("requires 'fields' to be specified"));
        }
    }

    public void testValidateEmptyLike() {
        String[] likeTexts = randomBoolean() ? null : new String[0];
        Item[] likeItems = randomBoolean() ? null : new Item[0];
        try {
            new MoreLikeThisQueryBuilder(likeTexts, likeItems);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("requires either 'like' texts or items to be specified"));
        }
    }

    public void testUnsupportedFields() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String unsupportedField = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
        MoreLikeThisQueryBuilder queryBuilder = new MoreLikeThisQueryBuilder(new String[] {unsupportedField}, new String[]{"some text"}, null)
                .failOnUnsupportedField(true);
        try {
            queryBuilder.toQuery(createShardContext());
            fail("should have failed with IllegalArgumentException for field: " + unsupportedField);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("more_like_this only supports text/keyword fields"));
        }
    }

    public void testMoreLikeThisBuilder() throws Exception {
        Query parsedQuery = parseQuery(moreLikeThisQuery(new String[]{"name.first", "name.last"}, new String[]{"something"}, null).minTermFreq(1).maxQueryTerms(12).buildAsBytes()).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    public void testItemSerialization() throws IOException {
        Item expectedItem = generateRandomItem();
        BytesStreamOutput output = new BytesStreamOutput();
        expectedItem.writeTo(output);
        Item newItem = new Item(StreamInput.wrap(output.bytes()));
        assertEquals(expectedItem, newItem);
    }

    public void testItemFromXContent() throws IOException {
        Item expectedItem = generateRandomItem();
        String json = expectedItem.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string();
        XContentParser parser = XContentFactory.xContent(json).createParser(json);
        Item newItem = Item.parse(parser, ParseFieldMatcher.STRICT, new Item());
        assertEquals(expectedItem, newItem);
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"more_like_this\" : {\n" +
                "    \"fields\" : [ \"title\", \"description\" ],\n" +
                "    \"like\" : [ \"and potentially some more text here as well\", {\n" +
                "      \"_index\" : \"imdb\",\n" +
                "      \"_type\" : \"movies\",\n" +
                "      \"_id\" : \"1\"\n" +
                "    }, {\n" +
                "      \"_index\" : \"imdb\",\n" +
                "      \"_type\" : \"movies\",\n" +
                "      \"_id\" : \"2\"\n" +
                "    } ],\n" +
                "    \"max_query_terms\" : 12,\n" +
                "    \"min_term_freq\" : 1,\n" +
                "    \"min_doc_freq\" : 5,\n" +
                "    \"max_doc_freq\" : 2147483647,\n" +
                "    \"min_word_length\" : 0,\n" +
                "    \"max_word_length\" : 0,\n" +
                "    \"minimum_should_match\" : \"30%\",\n" +
                "    \"boost_terms\" : 0.0,\n" +
                "    \"include\" : false,\n" +
                "    \"fail_on_unsupported_field\" : true,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        MoreLikeThisQueryBuilder parsed = (MoreLikeThisQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, parsed.fields().length);
        assertEquals(json, "and potentially some more text here as well", parsed.likeTexts()[0]);

        json =
                "{\n" +
                        "  \"mlt\" : {\n" +
                        "    \"fields\" : [ \"title\", \"description\" ],\n" +
                        "    \"like\" : [ \"and potentially some more text here as well\", {\n" +
                        "      \"_index\" : \"imdb\",\n" +
                        "      \"_type\" : \"movies\",\n" +
                        "      \"_id\" : \"1\"\n" +
                        "    }, {\n" +
                        "      \"_index\" : \"imdb\",\n" +
                        "      \"_type\" : \"movies\",\n" +
                        "      \"_id\" : \"2\"\n" +
                        "    } ],\n" +
                        "    \"max_query_terms\" : 12,\n" +
                        "    \"min_term_freq\" : 1,\n" +
                        "    \"min_doc_freq\" : 5,\n" +
                        "    \"max_doc_freq\" : 2147483647,\n" +
                        "    \"min_word_length\" : 0,\n" +
                        "    \"max_word_length\" : 0,\n" +
                        "    \"minimum_should_match\" : \"30%\",\n" +
                        "    \"boost_terms\" : 0.0,\n" +
                        "    \"include\" : false,\n" +
                        "    \"fail_on_unsupported_field\" : true,\n" +
                        "    \"boost\" : 1.0\n" +
                        "  }\n" +
                        "}";

        MoreLikeThisQueryBuilder parsedQueryMltShortcut = (MoreLikeThisQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        assertThat(parsedQueryMltShortcut, equalTo(parsed));

        try {
            parseQuery(json);
            fail("parse query should have failed in strict mode");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Deprecated field [mlt] used, expected [more_like_this] instead"));
        }
    }
}
