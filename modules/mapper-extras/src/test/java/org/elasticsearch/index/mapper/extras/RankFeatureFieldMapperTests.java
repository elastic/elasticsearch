/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.instanceOf;

public class RankFeatureFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return 10;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("positive_score_impact", b -> b.field("positive_score_impact", false));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 12.0));
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertEquals("_feature", termQuery.getTerm().field());
        assertEquals("field", termQuery.getTerm().text());
        assertNotNull(fields.getField("_feature"));
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        // always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    static int getFrequency(TokenStream tk) throws IOException {
        TermFrequencyAttribute freqAttribute = tk.addAttribute(TermFrequencyAttribute.class);
        tk.reset();
        assertTrue(tk.incrementToken());
        int freq = freqAttribute.getTermFrequency();
        assertFalse(tk.incrementToken());
        return freq;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "rank_feature");
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", 10)));
        List<IndexableField> fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields.get(0);

        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", 12)));
        FeatureField featureField2 = (FeatureField) doc2.rootDoc().getFields("_feature").get(0);

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    /**
     * Test method that uses parametrizedRankFeatureFieldTest method to cover all the test cases
     * Breakdown:
     * positiveScoreImpact is boolean so always possible cases 2
     *  nullValue is null possible cases 1:
     *      docs: List with null values or without so possible cases 2
     *      possible combinations 2*1*2 = 4
     *  nullValue is not null possible cases 1 (any normal float number it is not a different case):
     *      docs: List with null values and the other values, are all smaller than nullValue, are all bigger than nullValue,
     *      some are smaller some are bigger so 3 possible cases ( here it matters what are the other values because the frequencies
     *      that are calculated for documents with null value will use as their value the nullValue )
     *      possible combinations 2*1*3 = 6
     *  Total cases = 10
     */
    public void testAllFieldTypeCases() throws Exception {

        parametrizedRankFeatureFieldTest(false, null, Arrays.asList(12f, 13f));
        parametrizedRankFeatureFieldTest(true, null, Arrays.asList(12f, 13f));
        parametrizedRankFeatureFieldTest(false, null, Arrays.asList(12f, null, 13f, null));
        parametrizedRankFeatureFieldTest(true, null, Arrays.asList(12f, null, null, 13f));
        parametrizedRankFeatureFieldTest(false, 20f, Arrays.asList(5f, null, 6f, 45f));
        parametrizedRankFeatureFieldTest(true, 20f, Arrays.asList(5f, 6f, null, 45f, null));
        parametrizedRankFeatureFieldTest(false, 15f, Arrays.asList(2f, 5f, null, null));
        parametrizedRankFeatureFieldTest(false, 15f, Arrays.asList(20f, 50f, null, null, null));
        parametrizedRankFeatureFieldTest(true, 12f, Arrays.asList(1f, 3f, null, null, null));
        parametrizedRankFeatureFieldTest(true, 12f, Arrays.asList(null, null, 15f, 34f));

    }

    /**
     * Method that tests an exception is thrown when nullValue provided for rank feature is not a positive number
     */
    public void testExceptionIsThrownWhenNullValueNonPositive() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> parametrizedRankFeatureFieldTest(false, -2f, Arrays.asList(12f, 13f))
        );
        String message = "nullValue must be a positive normal float, for feature: [rank_feature] but it is "
            + Float.valueOf(-2f).toString()
            + " which is less than the minimum positive normal float: "
            + Float.MIN_NORMAL;
        String detailedMessage = "Failed to parse mapping: " + message;
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(message, e.getCause().getMessage());
    }

    /**
     * Provides parametrized test for the RankFeatureFieldType
     * Based on the input parameters produces and asserts a test scenario for the RankFeatureFieldType
     * Purpose of this method is to provide a convenient way to test multiple scenarios avoiding duplicated code
     *
     * @param  positiveScoreImpact  a boolean indicating whether the rank feature has a positive impact in score or not
     * @param  nullValue a normal positive Float number that can be null indicating the nullValue of the rank feature
     * @param docs a list of Float values that can be null indicating the documents values involved in the test scenario
     *
     * Example: given the following parameters set of parameters
     * (positiveScoreImpact, nullValue, docs) = (true, 12f, [10f, null, 20f])
     * All the assertions will succeed
     * This example represents the following use case: User has created an index providing a property with "type" : "rank_feature" and
     * parameters "positive_score_impact" : true "null_value" : 10f
     * User performs a bulk upload of three documents with values for the specified rank feature (doc1,doc2,doc3) = (10f, null, 20f)
     * User searches the index and all three documents return in response in the following order doc3, doc2, doc1
     * because the frequencies calculated for each doc will be freq3 > freq2 > freq1 {@link #getFrequency(TokenStream)}
     */
    private void parametrizedRankFeatureFieldTest(boolean positiveScoreImpact, Float nullValue, List<Float> docs) throws IOException {

        DocumentMapper mapper = nullValue == null
            ? createDocumentMapper(fieldMapping(b -> b.field("type", "rank_feature").field("positive_score_impact", positiveScoreImpact)))
            : createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "rank_feature").field("positive_score_impact", positiveScoreImpact).field("null_value", nullValue)
                )
            );

        List<FeatureField> featureFields = new ArrayList<>();
        List<Integer> frequencies = new ArrayList<>();

        for (Float value : docs) {
            ParsedDocument parsedDocument = mapper.parse(source(b -> b.field("field", value == null ? (byte[]) null : value)));
            List<IndexableField> fields = parsedDocument.rootDoc().getFields("_feature");
            // if no nullValue is provided and the value passed for rank_feature is null then fields should have zero size else 1
            assertEquals(value == null && nullValue == null ? 0 : 1, fields.size());
            if (value != null || nullValue != null) {
                assertThat(fields.get(0), instanceOf(FeatureField.class));
                featureFields.add((FeatureField) fields.get(0));
                frequencies.add(getFrequency(featureFields.get(featureFields.size() - 1).tokenStream(null, null)));
            }
            // no nullValue is provided and value is null aka no FeatureField is created and no frequency can calculate
            else {
                featureFields.add(null);
                frequencies.add(null);
            }
        }

        List<Float> valuesAfterNullReplaced;
        List<Integer> noNullFrequencies;
        // reverse the actual order of values provided if they impact negatively the score
        Comparator<Float> comparator = positiveScoreImpact ? (f1, f2) -> Float.compare(f2, f1) : Float::compare;

        // sort can't deal with null values
        if (nullValue == null) {
            valuesAfterNullReplaced = docs.stream().filter(Objects::nonNull).collect(Collectors.toList());
            noNullFrequencies = frequencies.stream().filter(Objects::nonNull).collect(Collectors.toList());
        } else {
            valuesAfterNullReplaced = docs.stream().map(value -> value == null ? nullValue : value).collect(Collectors.toList());
            noNullFrequencies = List.copyOf(frequencies);
        }

        // indexes in the original list of the sorted values
        List<Integer> indexes = valuesAfterNullReplaced.stream()
            .sorted(comparator)
            .map(valuesAfterNullReplaced::indexOf)
            .collect(Collectors.toList());
        // indexes in the
        List<Integer> indexes1 = noNullFrequencies.stream().sorted().map(noNullFrequencies::indexOf).collect(Collectors.toList());
        Collections.reverse(indexes1);
        assertEquals(indexes.size(), indexes1.size());

        // indexes should be the same aka the bigger the value the bigger the frequency if no negative impact on score
        // or the bigger the value the smaller the frequency if negative impact on score
        for (int i = 0; i < indexes.size(); i++) {
            assertEquals(indexes.get(i), indexes1.get(i));
        }

    }

    public void testRejectMultiValuedFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "rank_feature").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "rank_feature").endObject();
            }
            b.endObject().endObject();
        }));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", Arrays.asList(10, 20))))
        );
        assertEquals(
            "[rank_feature] fields do not support indexing multiple values for the same field [field] in the same document",
            e.getCause().getMessage()
        );

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().field("field", 10).endObject();
                b.startObject().field("field", 20).endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "[rank_feature] fields do not support indexing multiple values for the same field [foo.field] in the same document",
            e.getCause().getMessage()
        );
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
