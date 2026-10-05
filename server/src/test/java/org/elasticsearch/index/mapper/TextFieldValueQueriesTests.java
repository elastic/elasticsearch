/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.queries.BinaryDocValuesQueries;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The queries a {@code text} field answers over the values its doc values hold. They match a document's value whole,
 * which is what a predicate over the value asks for and what the field's own queries - matching the tokens the value
 * analyzes into - do not give.
 */
public class TextFieldValueQueriesTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of("the quick brown fox", "quick", "jumps over the lazy dog");

    public void testColumnarTextAnswersOverItsValues() throws IOException {
        assertWholeValueSemantics(columnar(b -> b.field("type", "text")));
    }

    /** A {@code text} field keeping its values outside the columnar modes answers them the same way. */
    public void testDocValuesTextAnswersOverItsValues() throws IOException {
        assertWholeValueSemantics(mapper(b -> {
            b.field("type", "text");
            b.field("doc_values", true);
        }));
    }

    /** A field keeping no values has none to answer over, which is what the caller asks before pushing a predicate. */
    public void testWithoutDocValuesThereAreNoValueQueries() throws IOException {
        final MapperService mapperService = mapper(b -> b.field("type", "text"));
        assertThat(valueQueries(mapperService), nullValue());
    }

    private void assertWholeValueSemantics(MapperService mapperService) throws IOException {
        final BinaryDocValuesQueries queries = valueQueries(mapperService);
        assertThat(queries, notNullValue());
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", doc))).rootDoc());
            }
        }, reader -> {
            final IndexSearcher searcher = newSearcher(reader);
            // The whole value, as a term and as a pattern.
            assertEquals("term over the whole value", 1, count(searcher, queries.term("field", new BytesRef("quick"))));
            assertEquals("the value starts with it", 1, count(searcher, queries.prefix("field", "the quick", false)));
            assertEquals("a pattern spanning the value", 1, count(searcher, queries.wildcard("field", "the quick*fox", false)));
            assertEquals(
                "and a regexp over it",
                2,
                count(searcher, queries.regexp("field", ".*the.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null))
            );
            // A token of a value is not the value: this is where these queries differ from the field's own.
            assertEquals("a lone token is not the value", 0, count(searcher, queries.term("field", new BytesRef("brown"))));
            assertEquals("nor is a token prefix", 0, count(searcher, queries.prefix("field", "brown", false)));
        });
    }

    private static int count(IndexSearcher searcher, Query query) throws IOException {
        return searcher.count(query);
    }

    private static BinaryDocValuesQueries valueQueries(MapperService mapperService) {
        return ((TextFamilyFieldType) mapperService.fieldType("field")).valueQueries();
    }

    private MapperService mapper(CheckedConsumer<XContentBuilder, IOException> field) throws IOException {
        return createMapperService(fieldMapping(field));
    }

    private MapperService columnar(CheckedConsumer<XContentBuilder, IOException> field) throws IOException {
        return createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            fieldMapping(field)
        );
    }
}
