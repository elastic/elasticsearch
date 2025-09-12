/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.IgnoredFieldsSpec;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class StoredFieldLoaderTests extends ESTestCase {
    private Document doc(String... values) {
        assert values.length % 2 == 0;
        Document doc = new Document();
        for (int i = 0; i < values.length; i++) {
            doc.add(new StoredField(values[i++], new BytesRef(values[i])));
        }
        return doc;
    }

    private StoredFieldsSpec fieldsSpec(
        Set<String> storedFields,
        Set<String> ignoredFields,
        IgnoredSourceFieldMapper.IgnoredSourceFormat format
    ) {
        return new StoredFieldsSpec(false, false, storedFields, new IgnoredFieldsSpec(ignoredFields, format));
    }

    public void testEmpty() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum", "_ignored_source.bar", "dolor sit amet"),
            fieldsSpec(Set.of(), Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, anEmptyMap());
            }
        );
    }

    public void testSingleIgnoredSourceNewFormat() throws IOException {
        testIgnoredSourceLoader(
            doc("_ignored_source.foo", "lorem ipsum"),
            fieldsSpec(Set.of(), Set.of("foo"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("_ignored_source.foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
            }
        );
    }

    public void testSingleIgnoredSourceOldFormat() throws IOException {
        testStoredFieldLoader(
            doc("_ignored_source", "lorem ipsum"),
            fieldsSpec(Set.of(), Set.of("foo"), IgnoredSourceFieldMapper.IgnoredSourceFormat.SINGLE_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("_ignored_source"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
            }
        );
    }

    public void testMultiValueIgnoredSourceNewFormat() throws IOException {
        testIgnoredSourceLoader(
            doc("_ignored_source.foo", "lorem ipsum", "_ignored_source.bar", "dolor sit amet"),
            fieldsSpec(Set.of(), Set.of("foo", "bar"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("_ignored_source.foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
                assertThat(storedFields, hasEntry(equalTo("_ignored_source.bar"), containsInAnyOrder(new BytesRef("dolor sit amet"))));
            }
        );
    }

    public void testMultiValueIgnoredSourceOldFormat() throws IOException {
        testStoredFieldLoader(
            doc("_ignored_source", "lorem ipsum", "_ignored_source", "dolor sit amet"),
            fieldsSpec(Set.of(), Set.of("foo", "bar"), IgnoredSourceFieldMapper.IgnoredSourceFormat.SINGLE_IGNORED_SOURCE),
            storedFields -> {
                assertThat(
                    storedFields,
                    hasEntry(equalTo("_ignored_source"), containsInAnyOrder(new BytesRef("lorem ipsum"), new BytesRef("dolor sit amet")))
                );
            }
        );
    }

    public void testSingleStoredField() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum"),
            fieldsSpec(Set.of("foo"), Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
            }
        );
    }

    public void testMultiValueStoredField() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum", "bar", "dolor sit amet"),
            fieldsSpec(Set.of("foo", "bar"), Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
                assertThat(storedFields, hasEntry(equalTo("bar"), containsInAnyOrder(new BytesRef("dolor sit amet"))));
            }
        );
    }

    public void testMixedStoredAndIgnoredFieldsNewFormat() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum", "_ignored_source.bar", "dolor sit amet"),
            fieldsSpec(Set.of("foo"), Set.of("bar"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
                assertThat(storedFields, hasEntry(equalTo("_ignored_source.bar"), containsInAnyOrder(new BytesRef("dolor sit amet"))));
            }
        );
    }

    public void testMixedStoredAndIgnoredFieldsOldFormat() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum", "_ignored_source", "dolor sit amet"),
            fieldsSpec(Set.of("foo"), Set.of("bar"), IgnoredSourceFieldMapper.IgnoredSourceFormat.SINGLE_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
                assertThat(storedFields, hasEntry(equalTo("_ignored_source"), containsInAnyOrder(new BytesRef("dolor sit amet"))));
            }
        );
    }

    public void testMixedStoredAndIgnoredFieldsLoadParent() throws IOException {
        testStoredFieldLoader(
            doc("foo", "lorem ipsum", "_ignored_source.parent", "dolor sit amet"),
            fieldsSpec(Set.of("foo"), Set.of("parent.bar"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE),
            storedFields -> {
                assertThat(storedFields, hasEntry(equalTo("foo"), containsInAnyOrder(new BytesRef("lorem ipsum"))));
                assertThat(storedFields, hasEntry(equalTo("_ignored_source.parent"), containsInAnyOrder(new BytesRef("dolor sit amet"))));
            }
        );
    }

    private void testStoredFieldLoader(Document doc, StoredFieldsSpec spec, Consumer<Map<String, List<Object>>> storedFieldsTest)
        throws IOException {
        testLoader(doc, spec, StoredFieldLoader.class, storedFieldsTest);
    }

    private void testIgnoredSourceLoader(Document doc, StoredFieldsSpec spec, Consumer<Map<String, List<Object>>> storedFieldsTest)
        throws IOException {
        testLoader(doc, spec, IgnoredSourceFieldLoader.class, storedFieldsTest);
    }

    private void testLoader(
        Document doc,
        StoredFieldsSpec spec,
        Class<? extends StoredFieldLoader> expectedLoaderClass,
        Consumer<Map<String, List<Object>>> storedFieldsTest
    ) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            iw.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                StoredFieldLoader loader = StoredFieldLoader.fromSpec(spec);
                assertThat(loader, Matchers.isA(expectedLoaderClass));
                var leafLoader = loader.getLoader(reader.leaves().getFirst(), new int[] { 0 });
                leafLoader.advanceTo(0);
                storedFieldsTest.accept(leafLoader.storedFields());
            }
        }
    }

}
