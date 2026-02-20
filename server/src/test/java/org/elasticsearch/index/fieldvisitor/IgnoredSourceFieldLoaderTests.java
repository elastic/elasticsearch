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
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Test that the {@link IgnoredSourceFieldLoader} loads the correct stored values.
 */
public class IgnoredSourceFieldLoaderTests extends ESTestCase {
    public void testSupports() {
        assertTrue(
            IgnoredSourceFieldLoader.supports(
                StoredFieldsSpec.withSourcePaths(
                    IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE,
                    Set.of("foo")
                )
            )
        );

        assertFalse(
            IgnoredSourceFieldLoader.supports(
                StoredFieldsSpec.withSourcePaths(IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE, Set.of())
            )
        );

        assertFalse(
            IgnoredSourceFieldLoader.supports(
                StoredFieldsSpec.withSourcePaths(IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE, Set.of("foo"))
            )
        );

        assertFalse(IgnoredSourceFieldLoader.supports(StoredFieldsSpec.NO_REQUIREMENTS));
    }

    private IgnoredSourceFieldMapper.NameValue[] nameValue(String name, String... values) {
        var nameValues = new IgnoredSourceFieldMapper.NameValue[values.length];
        for (int i = 0; i < values.length; i++) {
            nameValues[i] = new IgnoredSourceFieldMapper.NameValue(name, 0, new BytesRef(values[i]), null);
        }
        return nameValues;
    }

    public void testLoadSingle() throws IOException {
        var fooValue = nameValue("foo", "lorem ipsum");
        Document doc = new Document();
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(fooValue))));
        testLoader(doc, Set.of("foo"), ignoredSourceEntries -> {
            assertThat(ignoredSourceEntries, containsInAnyOrder(containsInAnyOrder(fooValue)));
        });
    }

    public void testLoadMultiple() throws IOException {
        var fooValue = nameValue("foo", "lorem ipsum");
        var barValue = nameValue("bar", "dolor sit amet");
        Document doc = new Document();
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(fooValue))));
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(barValue))));
        testLoader(doc, Set.of("foo", "bar"), ignoredSourceEntries -> {
            assertThat(ignoredSourceEntries, containsInAnyOrder(containsInAnyOrder(fooValue), containsInAnyOrder(barValue)));
        });
    }

    public void testLoadSubset() throws IOException {
        var fooValue = nameValue("foo", "lorem ipsum");
        var barValue = nameValue("bar", "dolor sit amet");

        Document doc = new Document();
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(fooValue))));
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(barValue))));

        testLoader(doc, Set.of("foo"), ignoredSourceEntries -> {
            assertThat(ignoredSourceEntries, containsInAnyOrder(containsInAnyOrder(fooValue)));
        });
    }

    public void testLoadFromParent() throws IOException {
        var fooValue = new IgnoredSourceFieldMapper.NameValue("parent", 7, new BytesRef("lorem ipsum"), null);
        Document doc = new Document();
        doc.add(new StoredField("_ignored_source", IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(fooValue))));
        testLoader(doc, Set.of("parent.foo"), ignoredSourceEntries -> {
            assertThat(ignoredSourceEntries, containsInAnyOrder(containsInAnyOrder(fooValue)));
        });
    }

    private void testLoader(
        Document doc,
        Set<String> fieldsToLoad,
        Consumer<List<List<IgnoredSourceFieldMapper.NameValue>>> ignoredSourceTest
    ) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            StoredFieldsSpec spec = StoredFieldsSpec.withSourcePaths(
                IgnoredSourceFieldMapper.IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE,
                fieldsToLoad
            );
            assertTrue(IgnoredSourceFieldLoader.supports(spec));
            iw.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                IgnoredSourceFieldLoader loader = new IgnoredSourceFieldLoader(spec, false);
                var leafLoader = loader.getLoader(reader.leaves().getFirst(), new int[] { 0 });
                leafLoader.advanceTo(0);
                @SuppressWarnings("unchecked")
                var ignoredSourceEntries = (List<List<IgnoredSourceFieldMapper.NameValue>>) (Object) leafLoader.storedFields()
                    .get("_ignored_source");
                ignoredSourceTest.accept(ignoredSourceEntries);
            }
        }
    }
}
