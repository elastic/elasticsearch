/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ExactMatchProcessorTests extends ESTestCase {

    public void testBasics() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                indexWriter.addDocument(createEnrichDocument("google.com", Map.of("globalRank", 1, "tldRank", 1, "tld", "com")));
                indexWriter.addDocument(createEnrichDocument("elastic.co", Map.of("globalRank", 451, "tldRank",23, "tld", "co")));
                indexWriter.addDocument(createEnrichDocument("bbc.co.uk", Map.of("globalRank", 45, "tldRank", 14, "tld", "co.uk")));
                indexWriter.addDocument(createEnrichDocument("eops.nl", Map.of("globalRank", 4567, "tldRank", 80, "tld", "nl")));
                indexWriter.commit();

                EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key",
                    List.of(), "schedule");
                Function<String, EnrichPolicy> policyLookup = policyName -> policy;

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "elastic.co"));
                    assertThat(processor.execute(ingestDocument), notNullValue());
                    assertThat(ingestDocument.getFieldValue("tld_rank", Integer.class), equalTo(23));
                    assertThat(ingestDocument.getFieldValue("tld", String.class), equalTo("co"));
                }

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "eops.nl"));
                    assertThat(processor.execute(ingestDocument), notNullValue());
                    assertThat(ingestDocument.getFieldValue("tld_rank", Integer.class), equalTo(80));
                    assertThat(ingestDocument.getFieldValue("tld", String.class), equalTo("nl"));
                }
            }
        }
    }

    public void testNoMatch() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                indexWriter.addDocument(createEnrichDocument("google.com", Map.of("globalRank", 1, "tldRank", 1, "tld", "com")));
                indexWriter.addDocument(createEnrichDocument("elastic.co", Map.of("globalRank", 451, "tldRank",23, "tld", "co")));
                indexWriter.addDocument(createEnrichDocument("bbc.co.uk", Map.of("globalRank", 45, "tldRank", 14, "tld", "co.uk")));
                indexWriter.addDocument(createEnrichDocument("eops.nl", Map.of("globalRank", 4567, "tldRank", 80, "tld", "nl")));
                indexWriter.commit();

                EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key",
                    List.of(), "schedule");
                Function<String, EnrichPolicy> policyLookup = policyName -> policy;

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "elastic.com"));
                    int numProperties = ingestDocument.getSourceAndMetadata().size();
                    assertThat(processor.execute(ingestDocument), notNullValue());
                    assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(numProperties));
                }
            }
        }
    }

    public void testMoreThanOneSegment() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                indexWriter.addDocument(createEnrichDocument("elastic.co", Map.of("globalRank", 451, "tldRank",23, "tld", "co")));
                indexWriter.commit();
                indexWriter.addDocument(createEnrichDocument("eops.nl", Map.of("globalRank", 4567, "tldRank", 80, "tld", "nl")));
                indexWriter.commit();

                EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key",
                    List.of(), "schedule");
                Function<String, EnrichPolicy> policyLookup = policyName -> policy;

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "elastic.co"));
                    Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
                    assertThat(e.getMessage(), equalTo("enrich index must have exactly a single segment"));
                }
            }
        }
    }

    public void testEmptyIndex() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                indexWriter.commit();

                EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key",
                    List.of(), "schedule");
                Function<String, EnrichPolicy> policyLookup = policyName -> policy;

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "elastic.co"));
                    int numProperties = ingestDocument.getSourceAndMetadata().size();
                    assertThat(processor.execute(ingestDocument), notNullValue());
                    assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(numProperties));
                }
            }
        }
    }

    public void testEnrichKeyFieldMissing() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                Document document = new Document();
                document.add(new StringField("different_key", "elastic.co", Field.Store.NO));
                indexWriter.addDocument(document);
                indexWriter.commit();

                EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key",
                    List.of(), "schedule");
                Function<String, EnrichPolicy> policyLookup = policyName -> policy;

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                    Function<String, Engine.Searcher> searchProvider = index -> new Engine.Searcher("_enrich", indexSearcher, indexReader);

                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", policyLookup, searchProvider, "_name", "domain", false,
                            List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Map.of("domain", "elastic.co"));
                    Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
                    assertThat(e.getMessage(), equalTo("enrich key field [key] does not exist"));
                }
            }
        }
    }

    public void testPolicyMissing() {
        Function<String, EnrichPolicy> policyLookup = policyName -> null;
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", policyLookup, indexExpression -> null, "_name", "domain",
            true, List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Map.of());
        expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
    }

    public void testIgnoreKeyMissing() throws Exception {
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("majestic_index"), "key", List.of(),
            "schedule");
        Function<String, EnrichPolicy> policyLookup = policyName -> policy;
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", policyLookup, indexExpression -> null, "_name", "domain",
                true, List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Map.of());

            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
            assertThat(processor.execute(ingestDocument), notNullValue());
            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
        }
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", policyLookup, indexExpression -> null, "_name", "domain",
                false, List.of(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Map.of());
            expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        }
    }

    private static Document createEnrichDocument(String key, Map<String, ?> decorateValues) throws IOException {
        BytesReference decorateContent;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
            builder.map(decorateValues);
            builder.flush();
            ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
            decorateContent = new BytesArray(outputStream.toByteArray());
        }
        Document document = new Document();
        document.add(new StringField("key", key, Field.Store.NO));
        document.add(new StoredField(SourceFieldMapper.NAME, decorateContent.toBytesRef()));
        return document;
    }

}
