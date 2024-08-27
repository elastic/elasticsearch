/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ScriptTermStatsTests extends ESTestCase {
    public void testMatchedTermsCount() throws IOException {
        // Returns number of matched terms for each doc.
        assertAllDocs(
            Set.of(new Term("field", "foo"), new Term("field", "bar")),
            ScriptTermStats::matchedTermsCount,
            Map.of("doc-1", equalTo(2), "doc-2", equalTo(2), "doc-3", equalTo(1))
        );

        // Partial match
        assertAllDocs(
            Set.of(new Term("field", "foo"), new Term("field", "baz")),
            ScriptTermStats::matchedTermsCount,
            Map.of("doc-1", equalTo(1), "doc-2", equalTo(1), "doc-3", equalTo(0))
        );

        // Always returns 0 when no term is provided.
        assertAllDocs(
            Set.of(),
            ScriptTermStats::matchedTermsCount,
            Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(0)))
        );

        // Always Returns 0 when none of the provided term has a match.
        assertAllDocs(
            randomTerms(),
            ScriptTermStats::matchedTermsCount,
            Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(0)))
        );

        // Always returns 0 when using a non-existing field
        assertAllDocs(
            Set.of(new Term("field-that-does-not-exists", "foo"), new Term("field-that-does-not-exists", "bar")),
            ScriptTermStats::matchedTermsCount,
            Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(0)))
        );
    }

    public void testDocFreq() throws IOException {
        // Single term
        {
            StatsSummary expected = new StatsSummary(1, 2, 2, 2);
            assertAllDocs(
                Set.of(new Term("field", "foo")),
                ScriptTermStats::docFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // Multiple terms
        {
            StatsSummary expected = new StatsSummary(2, 5, 2, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "bar")),
                ScriptTermStats::docFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // With missing terms
        {
            StatsSummary expected = new StatsSummary(2, 2, 0, 2);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "baz")),
                ScriptTermStats::docFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When no term is provided.
        {
            StatsSummary expected = new StatsSummary();
            assertAllDocs(
                Set.of(),
                ScriptTermStats::docFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When using a non-existing field
        {
            StatsSummary expected = new StatsSummary(2, 0, 0, 0);
            assertAllDocs(
                Set.of(new Term("non-existing-field", "foo"), new Term("non-existing-field", "baz")),
                ScriptTermStats::docFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }
    }

    public void testTotalTermFreq() throws IOException {
        // Single term
        {
            StatsSummary expected = new StatsSummary(1, 3, 3, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo")),
                ScriptTermStats::totalTermFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // Multiple terms
        {
            StatsSummary expected = new StatsSummary(2, 6, 3, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "bar")),
                ScriptTermStats::totalTermFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // With missing terms
        {
            StatsSummary expected = new StatsSummary(2, 3, 0, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "baz")),
                ScriptTermStats::totalTermFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When no term is provided.
        {
            StatsSummary expected = new StatsSummary();
            assertAllDocs(
                Set.of(),
                ScriptTermStats::totalTermFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When using a non-existing field
        {
            StatsSummary expected = new StatsSummary(2, 0, 0, 0);
            assertAllDocs(
                Set.of(new Term("non-existing-field", "foo"), new Term("non-existing-field", "baz")),
                ScriptTermStats::totalTermFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }
    }

    public void testTermFreq() throws IOException {
        // Single term
        {

            assertAllDocs(
                Set.of(new Term("field", "foo")),
                ScriptTermStats::termFreq,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(1, 1, 1, 1))),
                    Map.entry("doc-2", equalTo(new StatsSummary(1, 2, 2, 2))),
                    Map.entry("doc-3", equalTo(new StatsSummary(1, 0, 0, 0)))
                )
            );
        }

        // Multiple terms
        {
            StatsSummary expected = new StatsSummary(2, 6, 3, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "bar")),
                ScriptTermStats::termFreq,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(2, 2, 1, 1))),
                    Map.entry("doc-2", equalTo(new StatsSummary(2, 3, 1, 2))),
                    Map.entry("doc-3", equalTo(new StatsSummary(2, 1, 0, 1)))
                )
            );
        }

        // With missing terms
        {
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "baz")),
                ScriptTermStats::termFreq,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(2, 1, 0, 1))),
                    Map.entry("doc-2", equalTo(new StatsSummary(2, 2, 0, 2))),
                    Map.entry("doc-3", equalTo(new StatsSummary(2, 0, 0, 0)))
                )
            );
        }

        // When no term is provided.
        {
            StatsSummary expected = new StatsSummary();
            assertAllDocs(
                Set.of(),
                ScriptTermStats::termFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When using a non-existing field
        {
            StatsSummary expected = new StatsSummary(2, 0, 0, 0);
            assertAllDocs(
                Set.of(new Term("non-existing-field", "foo"), new Term("non-existing-field", "baz")),
                ScriptTermStats::termFreq,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }
    }

    public void testTermPositions() throws IOException {
        // Single term
        {

            assertAllDocs(
                Set.of(new Term("field", "foo")),
                ScriptTermStats::termPositions,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(1, 1, 1, 1))),
                    Map.entry("doc-2", equalTo(new StatsSummary(2, 3, 1, 2))),
                    Map.entry("doc-3", equalTo(new StatsSummary()))
                )
            );
        }

        // Multiple terms
        {
            StatsSummary expected = new StatsSummary(2, 6, 3, 3);
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "bar")),
                ScriptTermStats::termPositions,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(2, 3, 1, 2))),
                    Map.entry("doc-2", equalTo(new StatsSummary(3, 6, 1, 3))),
                    Map.entry("doc-3", equalTo(new StatsSummary(1, 1, 1, 1)))
                )
            );
        }

        // With missing terms
        {
            assertAllDocs(
                Set.of(new Term("field", "foo"), new Term("field", "baz")),
                ScriptTermStats::termPositions,
                Map.ofEntries(
                    Map.entry("doc-1", equalTo(new StatsSummary(1, 1, 1, 1))),
                    Map.entry("doc-2", equalTo(new StatsSummary(2, 3, 1, 2))),
                    Map.entry("doc-3", equalTo(new StatsSummary()))
                )
            );
        }

        // When no term is provided.
        {
            StatsSummary expected = new StatsSummary();
            assertAllDocs(
                Set.of(),
                ScriptTermStats::termPositions,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }

        // When using a non-existing field
        {
            StatsSummary expected = new StatsSummary();
            assertAllDocs(
                Set.of(new Term("non-existing-field", "foo"), new Term("non-existing-field", "bar")),
                ScriptTermStats::termPositions,
                Stream.of("doc-1", "doc-2", "doc-3").collect(Collectors.toMap(Function.identity(), k -> equalTo(expected)))
            );
        }
    }

    private void withIndexSearcher(CheckedConsumer<IndexSearcher, IOException> consummer) throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

            Document doc = new Document();
            doc.add(new TextField("id", "doc-1", Field.Store.YES));
            doc.add(new TextField("field", "foo bar", Field.Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("id", "doc-2", Field.Store.YES));
            doc.add(new TextField("field", "foo foo bar", Field.Store.YES));
            w.addDocument(doc);

            doc = new Document();
            doc.add(new TextField("id", "doc-3", Field.Store.YES));
            doc.add(new TextField("field", "bar", Field.Store.YES));
            w.addDocument(doc);

            try (IndexReader r = DirectoryReader.open(w)) {
                w.close();
                consummer.accept(newSearcher(r));
            }
        }
    }

    private <T> void assertAllDocs(Set<Term> terms, Function<ScriptTermStats, T> function, Map<String, Matcher<T>> expectedValues)
        throws IOException {
        withIndexSearcher(searcher -> {
            for (LeafReaderContext leafReaderContext : searcher.getLeafContexts()) {
                IndexReader reader = leafReaderContext.reader();
                DocIdSetIterator docIdSetIterator = DocIdSetIterator.all(reader.maxDoc());
                ScriptTermStats termStats = new ScriptTermStats(searcher, leafReaderContext, docIdSetIterator::docID, terms);
                while (docIdSetIterator.nextDoc() <= reader.maxDoc()) {
                    String docId = reader.document(docIdSetIterator.docID()).get("id");
                    if (expectedValues.containsKey(docId)) {
                        assertThat(function.apply(termStats), expectedValues.get(docId));
                    }
                }
            }
        });
    }

    private Set<Term> randomTerms() {
        Predicate<Term> isReservedTerm = term -> List.of("foo", "bar").contains(term.text());
        Supplier<Term> termSupplier = () -> randomValueOtherThanMany(
            isReservedTerm,
            () -> new Term("field", randomAlphaOfLengthBetween(1, 5))
        );
        return randomSet(1, randomIntBetween(1, 10), termSupplier);
    }
}
