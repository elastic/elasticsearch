/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WildcardFieldAggregationTests extends AggregatorTestCase {
    private static final String WILDCARD_FIELD_NAME = "wildcard_field";
    private static final int MAX_FIELD_LENGTH = 30;

    private WildcardFieldMapper wildcardFieldMapper;
    private WildcardFieldMapper.WildcardFieldType wildcardFieldType;


    @Before
    public void setup() {
        WildcardFieldMapper.Builder builder = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME, Version.CURRENT);
        builder.ignoreAbove(MAX_FIELD_LENGTH);
        wildcardFieldMapper = builder.build(new ContentPath(0));

        wildcardFieldType = wildcardFieldMapper.fieldType();
    }

    private void addFields(LuceneDocument parseDoc, Document doc, String docContent) throws IOException {
        ArrayList<IndexableField> fields = new ArrayList<>();
        wildcardFieldMapper.createFields(docContent, parseDoc, fields);

        for (IndexableField indexableField : fields) {
            doc.add(indexableField);
        }
    }

    private void indexDoc(LuceneDocument parseDoc, Document doc, RandomIndexWriter iw) throws IOException {
        IndexableField field = parseDoc.getByKey(wildcardFieldMapper.name());
        if (field != null) {
            doc.add(field);
        }
        iw.addDocument(doc);
    }

    private void indexStrings(RandomIndexWriter iw, String... values) throws IOException {
        Document doc = new Document();
        LuceneDocument parseDoc = new LuceneDocument();
        for (String value : values) {
            addFields(parseDoc, doc, value);
        }
        indexDoc(parseDoc, doc, iw);
    }

    public void testTermsAggregation() throws IOException {
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name")
            .field(WILDCARD_FIELD_NAME)
            .order(BucketOrder.key(true));

        testCase(aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                indexStrings(iw, "a");
                indexStrings(iw, "a");
                indexStrings(iw, "b");
                indexStrings(iw, "b");
                indexStrings(iw, "b");
                indexStrings(iw, "c");
            },
            (StringTerms result) -> {
                assertTrue(AggregationInspectionHelper.hasValue(result));

                assertEquals(3, result.getBuckets().size());
                assertEquals("a", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("b", result.getBuckets().get(1).getKeyAsString());
                assertEquals(3L, result.getBuckets().get(1).getDocCount());
                assertEquals("c", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            },
            wildcardFieldType);
    }

    public void testCompositeTermsAggregation() throws IOException {
        CompositeAggregationBuilder aggregationBuilder = new CompositeAggregationBuilder(
            "name",
            List.of(
                new TermsValuesSourceBuilder("terms_key").field(WILDCARD_FIELD_NAME)
            )
        );

        testCase(aggregationBuilder,
            new MatchAllDocsQuery(),
            iw -> {
                indexStrings(iw, "a");
                indexStrings(iw, "c");
                indexStrings(iw, "a");
                indexStrings(iw, "d");
                indexStrings(iw, "c");
            },
            (InternalComposite result) -> {
                assertTrue(AggregationInspectionHelper.hasValue(result));

                assertEquals(3, result.getBuckets().size());
                assertEquals("{terms_key=d}", result.afterKey().toString());
                assertEquals("{terms_key=a}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{terms_key=c}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(1).getDocCount());
                assertEquals("{terms_key=d}", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            },
            wildcardFieldType);
    }

    public void testCompositeTermsSearchAfter() throws IOException {

        TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder("terms_key").field(WILDCARD_FIELD_NAME);
        CompositeAggregationBuilder aggregationBuilder = new CompositeAggregationBuilder("name", Collections.singletonList(terms))
            .aggregateAfter(Collections.singletonMap("terms_key", "a"));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            indexStrings(iw, "a");
            indexStrings(iw, "c");
            indexStrings(iw, "a");
            indexStrings(iw, "d");
            indexStrings(iw, "c");
        }, (InternalComposite result) -> {
            assertEquals(2, result.getBuckets().size());
            assertEquals("{terms_key=d}", result.afterKey().toString());
            assertEquals("{terms_key=c}", result.getBuckets().get(0).getKeyAsString());
            assertEquals(2L, result.getBuckets().get(0).getDocCount());
            assertEquals("{terms_key=d}", result.getBuckets().get(1).getKeyAsString());
            assertEquals(1L, result.getBuckets().get(1).getDocCount());
        }, wildcardFieldType);
    }
}
