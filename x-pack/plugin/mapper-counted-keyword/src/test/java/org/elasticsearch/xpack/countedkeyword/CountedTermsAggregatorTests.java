/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.countedterms.CountedTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CountedTermsAggregatorTests extends AggregatorTestCase {
    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new CountedKeywordMapperPlugin());
    }

    public void testAggregatesCountedKeywords() throws Exception {
        FieldMapper mapper = new CountedKeywordFieldMapper.Builder("stacktraces", Mapper.SourceKeepMode.NONE).build(
            MapperBuilderContext.root(false, false)
        );
        MappedFieldType fieldType = mapper.fieldType();

        CountedTermsAggregationBuilder aggregationBuilder = new CountedTermsAggregationBuilder("st").field("stacktraces");
        testCase(iw -> {
            iw.addDocument(doc(mapper, "a", null, "a", "b"));
            iw.addDocument(doc(mapper, "b", "c", "d"));
            iw.addDocument(doc(mapper, new String[] { null }));

        }, (InternalTerms<?, ?> result) -> {
            // note how any nulls are ignored
            Map<String, Long> expectedBuckets = Map.of("a", 2L, "b", 2L, "c", 1L, "d", 1L);
            assertEquals("Bucket count does not match", expectedBuckets.size(), result.getBuckets().size());

            Set<String> seenUniqueKeys = new HashSet<>();
            for (InternalTerms.Bucket<?> bucket : result.getBuckets()) {
                String k = bucket.getKeyAsString();
                assertTrue("Unexpected bucket key [" + k + "]", expectedBuckets.containsKey(k));
                assertEquals(expectedBuckets.get(k).longValue(), bucket.getDocCount());
                seenUniqueKeys.add(k);
            }
            // ensure no duplicate keys
            assertEquals("Every bucket key must be unique", expectedBuckets.size(), seenUniqueKeys.size());
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }, new AggTestConfig(aggregationBuilder, fieldType));
    }

    private List<IndexableField> doc(FieldMapper mapper, String... values) {
        // quote regular strings but keep null values unquoted so they are not treated as regular strings
        List<String> quotedValues = Arrays.stream(values).map(v -> v != null ? "\"" + v + "\"" : v).toList();
        String source = "[" + Strings.collectionToCommaDelimitedString(quotedValues) + "]";
        try {
            XContentParser parser = createParser(JsonXContent.jsonXContent, source);
            // move to first token
            parser.nextToken();
            TestDocumentParserContext ctx = new TestDocumentParserContext(
                MappingLookup.EMPTY,
                new SourceToParse("test", new BytesArray(source), XContentType.JSON)
            ) {
                @Override
                public XContentParser parser() {
                    return parser;
                }
            };
            mapper.parse(ctx);
            return ctx.doc().getFields();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
