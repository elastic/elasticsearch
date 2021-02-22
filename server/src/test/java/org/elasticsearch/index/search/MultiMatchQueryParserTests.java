/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryParserTests extends ESSingleNodeTestCase {

    public void testKeywordSplitQueriesOnWhitespace() throws IOException {
        IndexService indexService = createIndex("test_keyword", Settings.builder()
            .put("index.analysis.normalizer.my_lowercase.type", "custom")
            .putList("index.analysis.normalizer.my_lowercase.filter", "lowercase").build());
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("field_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                    .endObject()
                    .startObject("field_split")
                        .field("type", "keyword")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                    .startObject("field_split_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
            .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null, () -> { throw new UnsupportedOperationException(); },
            null,
            emptyMap()
        );
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("field", 1.0f);
        fieldNames.put("field_split", 1.0f);
        fieldNames.put("field_normalizer", 1.0f);
        fieldNames.put("field_split_normalizer", 1.0f);
        Query query = parser.parse(MultiMatchQueryBuilder.Type.BEST_FIELDS, fieldNames, "Foo Bar", null);
        DisjunctionMaxQuery expected = new DisjunctionMaxQuery(
            Arrays.asList(
                new TermQuery(new Term("field_normalizer", "foo bar")),
                new TermQuery(new Term("field", "Foo Bar")),
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("field_split", "Foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split", "Bar")), BooleanClause.Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("field_split_normalizer", "foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split_normalizer", "bar")), BooleanClause.Occur.SHOULD)
                    .build()
        ), 0.0f);
        assertThat(query, equalTo(expected));
    }
}
