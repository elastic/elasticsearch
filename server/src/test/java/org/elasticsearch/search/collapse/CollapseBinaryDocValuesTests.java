/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.collapse;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Collapse driven through {@link CollapseBuilder#build(SearchExecutionContext)}, so the decoder is the one the mapping
 * selects rather than one the test names.
 *
 * <p>These fixtures are {@code SEPARATE_COUNT}, which stores a lone value raw, so a single-valued document decodes the
 * same through a plain decoder and only the multi-value rejection fails a wrong binding. An integrated-count blob keeps
 * its framing even for a lone value, which is why the pre-gate cases also discriminate.
 */
public class CollapseBinaryDocValuesTests extends ESTestCase {

    private static final String FIELD = "host.name";
    private static final String SORT_FIELD = "sort";

    public void testGroupsRepeatedValuesAcrossDocuments() throws IOException {
        List<List<String>> docs = List.of(Arrays.asList("host-a"), Arrays.asList("host-b"), Arrays.asList("host-a"));
        TopFieldGroups groups = collapse(docs, IndexVersion.current());

        assertEquals(2, groups.groupValues.length);
        assertEquals(new BytesRef("host-a"), groups.groupValues[0]);
        assertEquals(new BytesRef("host-b"), groups.groupValues[1]);
    }

    public void testRejectsTwoNonNullValues() throws IOException {
        List<List<String>> docs = List.of(Arrays.asList("host-a", "host-b"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> collapse(docs, IndexVersion.current()));
        assertEquals("failed to extract doc:0, the grouping field must be single valued", e.getMessage());
    }

    public void testEmptyStringIsItsOwnGroupAndMissingValuesGroupOnNull() throws IOException {
        List<List<String>> docs = List.of(
            Arrays.asList(""),
            Arrays.asList("host-a"),
            Collections.emptyList(),
            Arrays.asList((String) null)
        );
        TopFieldGroups groups = collapse(docs, IndexVersion.current());

        assertEquals(3, groups.groupValues.length);
        assertEquals(new BytesRef(""), groups.groupValues[0]);
        assertEquals(new BytesRef("host-a"), groups.groupValues[1]);
        assertNull(groups.groupValues[2]);
    }

    public void testReadsIntegratedCountsWhenTheIndexPredatesTheCountsCompanion() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);
        List<List<String>> docs = List.of(Arrays.asList("host-a"), Arrays.asList("host-b"), Arrays.asList("host-a"));
        TopFieldGroups groups = collapse(docs, oldVersion);

        assertEquals(2, groups.groupValues.length);
        assertEquals(new BytesRef("host-a"), groups.groupValues[0]);
        assertEquals(new BytesRef("host-b"), groups.groupValues[1]);
    }

    public void testRejectsTwoNonNullValuesWhenTheIndexPredatesTheCountsCompanion() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);
        List<List<String>> docs = List.of(Arrays.asList("host-a", "host-b"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> collapse(docs, oldVersion));
        assertEquals("failed to extract doc:0, the grouping field must be single valued", e.getMessage());
    }

    private static TopFieldGroups collapse(List<List<String>> valuesPerDoc, IndexVersion indexVersion) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < valuesPerDoc.size(); i++) {
                    LuceneDocument doc = new LuceneDocument();
                    for (String value : valuesPerDoc.get(i)) {
                        if (value != null) {
                            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                                doc,
                                FIELD,
                                new BytesRef(value),
                                MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED,
                                indexVersion
                            );
                        }
                    }
                    doc.add(new SortedNumericDocValuesField(SORT_FIELD, i));
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                SinglePassGroupingCollector<?> collector = collapseContext(indexVersion).createTopDocs(
                    new Sort(new SortedNumericSortField(SORT_FIELD, SortField.Type.LONG)),
                    10,
                    null
                );
                searcher.search(Queries.ALL_DOCS_INSTANCE, collector);
                return collector.getTopGroups(0);
            }
        }
    }

    private static CollapseContext collapseContext(IndexVersion indexVersion) {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(FIELD, true, true, true, Collections.emptyMap());
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getFieldType(FIELD)).thenReturn(fieldType);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(indexVersion);
        return new CollapseBuilder(FIELD).build(searchExecutionContext);
    }
}
