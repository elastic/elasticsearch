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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.IgnoredFieldsSpec;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Test that the {@link IgnoredSourceFieldLoader} loads the correct stored values.
 */
public class IgnoredSourceFieldLoaderTests extends ESTestCase {
    public void testSupports() {
        assertTrue(
            IgnoredSourceFieldLoader.supports(
                new BlockLoader.FieldsSpec(
                    StoredFieldsSpec.NO_REQUIREMENTS,
                    new IgnoredFieldsSpec(Set.of("foo"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE)
                )
            )
        );

        assertTrue(
            IgnoredSourceFieldLoader.supports(
                new BlockLoader.FieldsSpec(
                    StoredFieldsSpec.NO_REQUIREMENTS,
                    new IgnoredFieldsSpec(Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE)
                )
            )
        );

        assertFalse(
            IgnoredSourceFieldLoader.supports(
                new BlockLoader.FieldsSpec(
                    StoredFieldsSpec.NEEDS_SOURCE,
                    new IgnoredFieldsSpec(Set.of("foo"), IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE)
                )
            )
        );

        assertFalse(
            IgnoredSourceFieldLoader.supports(new BlockLoader.FieldsSpec(StoredFieldsSpec.NO_REQUIREMENTS, IgnoredFieldsSpec.NONE))
        );
    }

    public void testLoadSingle() throws IOException {
        // Note: normally the stored value is encoded in the ignored source format
        // (see IgnoredSourceFieldMapper#encodeMultipleValuesForField), but these tests are only verifying the loader, not the encoding.
        BytesRef value = new BytesRef("lorem ipsum");
        Document doc = new Document();
        doc.add(new StoredField("_ignored_source.foo", value));
        testLoader(doc, Set.of("foo"), storedFields -> {
            assertThat(storedFields, hasEntry(equalTo("_ignored_source.foo"), containsInAnyOrder(value)));
        });
    }

    public void testLoaderNone() throws IOException {
        testLoader(new Document(), Set.of(), storedFields -> { assertThat(storedFields, anEmptyMap()); });
    }

    public void testLoadMultiple() throws IOException {
        BytesRef fooValue = new BytesRef("lorem ipsum");
        BytesRef barValue = new BytesRef("dolor sit amet");
        Document doc = new Document();
        doc.add(new StoredField("_ignored_source.foo", fooValue));
        doc.add(new StoredField("_ignored_source.bar", barValue));
        testLoader(doc, Set.of("foo", "bar"), storedFields -> {
            assertThat(storedFields, hasEntry(equalTo("_ignored_source.foo"), containsInAnyOrder(fooValue)));
            assertThat(storedFields, hasEntry(equalTo("_ignored_source.bar"), containsInAnyOrder(barValue)));
        });
    }

    public void testLoadSubset() throws IOException {
        BytesRef fooValue = new BytesRef("lorem ipsum");
        BytesRef barValue = new BytesRef("dolor sit amet");

        Document doc = new Document();
        doc.add(new StoredField("_ignored_source.foo", fooValue));
        doc.add(new StoredField("_ignored_source.bar", barValue));

        testLoader(doc, Set.of("foo"), storedFields -> {
            assertThat(storedFields, hasEntry(equalTo("_ignored_source.foo"), containsInAnyOrder(fooValue)));
            assertThat(storedFields, not(hasKey("_ignored_source.bar")));
        });
    }

    private void testLoader(Document doc, Set<String> fieldsToLoad, Consumer<Map<String, List<Object>>> storedFieldsConsumer)
        throws IOException {
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            BlockLoader.FieldsSpec spec = new BlockLoader.FieldsSpec(
                StoredFieldsSpec.NO_REQUIREMENTS,
                new IgnoredFieldsSpec(fieldsToLoad, IgnoredSourceFieldMapper.IgnoredSourceFormat.PER_FIELD_IGNORED_SOURCE)
            );
            assertTrue(IgnoredSourceFieldLoader.supports(spec));
            iw.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                IgnoredSourceFieldLoader loader = new IgnoredSourceFieldLoader(spec, false);
                var leafLoader = loader.getLoader(reader.leaves().getFirst(), new int[] { 0 });
                leafLoader.advanceTo(0);
                storedFieldsConsumer.accept(leafLoader.storedFields());
            }
        }
    }
}
