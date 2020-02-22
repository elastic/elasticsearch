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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.NotEqualMessageBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;

public class InternalTopHitsTests extends InternalAggregationTestCase<InternalTopHits> {
    /**
     * Should the test instances look like they are sorted by some fields (true) or sorted by score (false). Set here because these need
     * to be the same across the entirety of {@link #testReduceRandom()}.
     */
    private boolean testInstancesLookSortedByField;
    /**
     * Fields shared by all instances created by {@link #createTestInstance(String, List, Map)}.
     */
    private SortField[] testInstancesSortFields;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testInstancesLookSortedByField = randomBoolean();
        testInstancesSortFields = testInstancesLookSortedByField ? randomSortFields() : new SortField[0];
    }

    @Override
    protected InternalTopHits createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        int from = 0;
        int requestedSize = between(1, 40);
        int actualSize = between(0, requestedSize);

        float maxScore = Float.NEGATIVE_INFINITY;
        ScoreDoc[] scoreDocs = new ScoreDoc[actualSize];
        SearchHit[] hits = new SearchHit[actualSize];
        Set<Integer> usedDocIds = new HashSet<>();
        for (int i = 0; i < actualSize; i++) {
            float score = randomFloat();
            maxScore = max(maxScore, score);
            int docId = randomValueOtherThanMany(usedDocIds::contains, () -> between(0, IndexWriter.MAX_DOCS));
            usedDocIds.add(docId);

            Map<String, DocumentField> searchHitFields = new HashMap<>();
            if (testInstancesLookSortedByField) {
                Object[] fields = new Object[testInstancesSortFields.length];
                for (int f = 0; f < testInstancesSortFields.length; f++) {
                    fields[f] = randomOfType(testInstancesSortFields[f].getType());
                }
                scoreDocs[i] = new FieldDoc(docId, score, fields);
            } else {
                scoreDocs[i] = new ScoreDoc(docId, score);
            }
            hits[i] = new SearchHit(docId, Integer.toString(i), searchHitFields);
            hits[i].score(score);
        }
        int totalHits = between(actualSize, 500000);
        SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);

        TopDocs topDocs;
        Arrays.sort(scoreDocs, scoreDocComparator());
        if (testInstancesLookSortedByField) {
            topDocs = new TopFieldDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs, testInstancesSortFields);
        } else {
            topDocs = new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
        }
        // Lucene's TopDocs initializes the maxScore to Float.NaN, if there is no maxScore
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore == Float.NEGATIVE_INFINITY ? Float.NaN : maxScore);

        return new InternalTopHits(name, from, requestedSize, topDocsAndMaxScore, searchHits, pipelineAggregators, metaData);
    }

    @Override
    protected void assertFromXContent(InternalTopHits aggregation, ParsedAggregation parsedAggregation) throws IOException {
        final SearchHits expectedSearchHits = aggregation.getHits();

        assertTrue(parsedAggregation instanceof ParsedTopHits);
        ParsedTopHits parsed = (ParsedTopHits) parsedAggregation;
        final SearchHits actualSearchHits = parsed.getHits();

        assertEquals(expectedSearchHits.getTotalHits().value, actualSearchHits.getTotalHits().value);
        assertEquals(expectedSearchHits.getTotalHits().relation, actualSearchHits.getTotalHits().relation);
        assertEquals(expectedSearchHits.getMaxScore(), actualSearchHits.getMaxScore(), 0.0f);

        List<SearchHit> expectedHits = Arrays.asList(expectedSearchHits.getHits());
        List<SearchHit> actualHits = Arrays.asList(actualSearchHits.getHits());

        assertEquals(expectedHits.size(), actualHits.size());
        for (int i = 0; i < expectedHits.size(); i++) {
            SearchHit expected = expectedHits.get(i);
            SearchHit actual = actualHits.get(i);

            assertEquals(expected.getIndex(), actual.getIndex());
            assertEquals(expected.getId(), actual.getId());
            assertEquals(expected.getVersion(), actual.getVersion());
            assertEquals(expected.getScore(), actual.getScore(), 0.0f);
            assertEquals(expected.getFields(), actual.getFields());
            assertEquals(expected.getSourceAsMap(), actual.getSourceAsMap());
        }
    }

    private Object randomOfType(SortField.Type type) {
        switch (type) {
        case CUSTOM:
            throw new UnsupportedOperationException();
        case DOC:
            return between(0, IndexWriter.MAX_DOCS);
        case DOUBLE:
            return randomDouble();
        case FLOAT:
            return randomFloat();
        case INT:
            return randomInt();
        case LONG:
            return randomLong();
        case REWRITEABLE:
            throw new UnsupportedOperationException();
        case SCORE:
            return randomFloat();
        case STRING:
            return new BytesRef(randomAlphaOfLength(5));
        case STRING_VAL:
            return new BytesRef(randomAlphaOfLength(5));
        default:
            throw new UnsupportedOperationException("Unknown SortField.Type: " + type);
        }
    }

    @Override
    protected void assertReduced(InternalTopHits reduced, List<InternalTopHits> inputs) {
        SearchHits actualHits = reduced.getHits();
        List<Tuple<ScoreDoc, SearchHit>> allHits = new ArrayList<>();
        float maxScore = Float.NEGATIVE_INFINITY;
        long totalHits = 0;
        TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
        for (int input = 0; input < inputs.size(); input++) {
            SearchHits internalHits = inputs.get(input).getHits();
            totalHits += internalHits.getTotalHits().value;
            if (internalHits.getTotalHits().relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
                relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
            maxScore = max(maxScore, internalHits.getMaxScore());
            for (int i = 0; i < internalHits.getHits().length; i++) {
                ScoreDoc doc = inputs.get(input).getTopDocs().topDocs.scoreDocs[i];
                if (testInstancesLookSortedByField) {
                    doc = new FieldDoc(doc.doc, doc.score, ((FieldDoc) doc).fields, input);
                } else {
                    doc = new ScoreDoc(doc.doc, doc.score, input);
                }
                allHits.add(new Tuple<>(doc, internalHits.getHits()[i]));
            }
        }
        allHits.sort(comparing(Tuple::v1, scoreDocComparator()));
        SearchHit[] expectedHitsHits = new SearchHit[min(inputs.get(0).getSize(), allHits.size())];
        for (int i = 0; i < expectedHitsHits.length; i++) {
            expectedHitsHits[i] = allHits.get(i).v2();
        }
        // Lucene's TopDocs initializes the maxScore to Float.NaN, if there is no maxScore
        SearchHits expectedHits = new SearchHits(expectedHitsHits, new TotalHits(totalHits, relation), maxScore == Float.NEGATIVE_INFINITY ?
            Float.NaN :
            maxScore);
        assertEqualsWithErrorMessageFromXContent(expectedHits, actualHits);
    }

    /**
     * Assert that two objects are equals, calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} to print out their
     * differences if they aren't equal.
     */
    private static <T extends ToXContent> void assertEqualsWithErrorMessageFromXContent(T expected, T actual) {
        if (Objects.equals(expected, actual)) {
            return;
        }
        if (expected == null) {
            throw new AssertionError("Expected null be actual was [" + actual.toString() + "]");
        }
        if (actual == null) {
            throw new AssertionError("Didn't expect null but actual was [null]");
        }
        try (XContentBuilder actualJson = JsonXContent.contentBuilder();
             XContentBuilder expectedJson = JsonXContent.contentBuilder()) {
            actualJson.startObject();
            actual.toXContent(actualJson, ToXContent.EMPTY_PARAMS);
            actualJson.endObject();
            expectedJson.startObject();
            expected.toXContent(expectedJson, ToXContent.EMPTY_PARAMS);
            expectedJson.endObject();
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(
                XContentHelper.convertToMap(BytesReference.bytes(actualJson), false).v2(),
                XContentHelper.convertToMap(BytesReference.bytes(expectedJson), false).v2());
            throw new AssertionError("Didn't match expected value:\n" + message);
        } catch (IOException e) {
            throw new AssertionError("IOException while building failure message", e);
        }
    }

    @Override
    protected Reader<InternalTopHits> instanceReader() {
        return InternalTopHits::new;
    }

    private SortField[] randomSortFields() {
        SortField[] sortFields = new SortField[between(1, 5)];
        Set<String> usedSortFields = new HashSet<>();
        for (int i = 0; i < sortFields.length; i++) {
            String sortField = randomValueOtherThanMany(usedSortFields::contains, () -> randomAlphaOfLength(5));
            usedSortFields.add(sortField);
            SortField.Type type = randomValueOtherThanMany(t -> t == SortField.Type.CUSTOM || t == SortField.Type.REWRITEABLE,
                    () -> randomFrom(SortField.Type.values()));
            sortFields[i] = new SortField(sortField, type);
        }
        return sortFields;
    }

    private Comparator<ScoreDoc> scoreDocComparator() {
        return innerScoreDocComparator().thenComparing(s -> s.shardIndex);
    }

    private Comparator<ScoreDoc> innerScoreDocComparator() {
        if (testInstancesLookSortedByField) {
            // Values passed to getComparator shouldn't matter
            @SuppressWarnings("rawtypes")
            FieldComparator[] comparators = new FieldComparator[testInstancesSortFields.length];
            for (int i = 0; i < testInstancesSortFields.length; i++) {
                comparators[i] = testInstancesSortFields[i].getComparator(0, 0);
            }
            return (lhs, rhs) -> {
                FieldDoc l = (FieldDoc) lhs;
                FieldDoc r = (FieldDoc) rhs;
                int i = 0;
                while (i < l.fields.length) {
                    @SuppressWarnings("unchecked")
                    int c = comparators[i].compareValues(l.fields[i], r.fields[i]);
                    if (c != 0) {
                        return c;
                    }
                    i++;
                }
                return 0;
            };
        } else {
            Comparator<ScoreDoc> comparator = comparing(d -> d.score);
            return comparator.reversed();
        }
    }

    @Override
    protected InternalTopHits mutateInstance(InternalTopHits instance) {
        String name = instance.getName();
        int from = instance.getFrom();
        int size = instance.getSize();
        TopDocsAndMaxScore topDocs = instance.getTopDocs();
        SearchHits searchHits = instance.getHits();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 5)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            from += between(1, 100);
            break;
        case 2:
            size += between(1, 100);
            break;
        case 3:
            topDocs = new TopDocsAndMaxScore(new TopDocs(new TotalHits(topDocs.topDocs.totalHits.value + between(1, 100),
                    topDocs.topDocs.totalHits.relation), topDocs.topDocs.scoreDocs), topDocs.maxScore + randomFloat());
            break;
        case 4:
            TotalHits totalHits = new TotalHits(searchHits.getTotalHits().value + between(1, 100), randomFrom(TotalHits.Relation.values()));
            searchHits = new SearchHits(searchHits.getHits(), totalHits, searchHits.getMaxScore() + randomFloat());
            break;
        case 5:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalTopHits(name, from, size, topDocs, searchHits, pipelineAggregators, metaData);
    }
}
