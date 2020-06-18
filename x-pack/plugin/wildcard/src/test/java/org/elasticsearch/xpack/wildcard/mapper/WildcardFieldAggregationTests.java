/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Consumer;

public class WildcardFieldAggregationTests extends AggregatorTestCase {
    private static final String WILDCARD_FIELD_NAME = "wildcard_field";
    private static final int MAX_FIELD_LENGTH = 30;

    private Optional<IndexWriterConfig> iwc;
    private WildcardFieldMapper wildcardFieldMapper;
    private WildcardFieldMapper.WildcardFieldType wildcardFieldType;


    @Before
    public void setup() {
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        this.iwc = Optional.of(iwc);

        WildcardFieldMapper.Builder builder = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME);
        builder.ignoreAbove(MAX_FIELD_LENGTH);
        wildcardFieldMapper = builder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));

        wildcardFieldType = wildcardFieldMapper.fieldType();
    }

    private void addFields(ParseContext.Document parseDoc, Document doc, String docContent) throws IOException {
        ArrayList<IndexableField> fields = new ArrayList<>();
        wildcardFieldMapper.createFields(docContent, parseDoc, fields);

        for (IndexableField indexableField : fields) {
            doc.add(indexableField);
        }
    }

    private void indexDoc(ParseContext.Document parseDoc, Document doc, RandomIndexWriter iw) throws IOException {
        IndexableField field = parseDoc.getByKey(wildcardFieldMapper.name());
        if (field != null) {
            doc.add(field);
        }
        iw.addDocument(doc);
    }

    private void indexStrings(RandomIndexWriter iw, String... values) throws IOException {
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
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
            iwc, new MatchAllDocsQuery(),
            iw -> {
                indexStrings(iw, "a");
                indexStrings(iw, "a");
                indexStrings(iw, "b");
                indexStrings(iw, "b");
                indexStrings(iw, "b");
                indexStrings(iw, "c");
            },
            (Consumer<StringTerms>) result -> {
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
}
