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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.enrich.ExactMatchProcessor.ENRICH_KEY_FIELD_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ExactMatchProcessorTests extends ESTestCase {

    public void testBasics() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig iwConfig = new IndexWriterConfig(new MockAnalyzer(random()));
            iwConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(directory, iwConfig)) {
                indexWriter.addDocument(createEnrichDocument("google.com", "globalRank", 1, "tldRank", 1, "tld", "com"));
                indexWriter.addDocument(createEnrichDocument("elastic.co", "globalRank", 451, "tldRank",23, "tld", "co"));
                indexWriter.addDocument(createEnrichDocument("bbc.co.uk", "globalRank", 45, "tldRank", 14, "tld", "co.uk"));
                indexWriter.addDocument(createEnrichDocument("eops.nl", "globalRank", 4567, "tldRank", 80, "tld", "nl"));
                indexWriter.commit();

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                                Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "elastic.co"));
                    assertThat(processor.execute(ingestDocument), notNullValue());
                    assertThat(ingestDocument.getFieldValue("tld_rank", Integer.class), equalTo(23));
                    assertThat(ingestDocument.getFieldValue("tld", String.class), equalTo("co"));
                }

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "eops.nl"));
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
                indexWriter.addDocument(createEnrichDocument("google.com", "globalRank", 1, "tldRank", 1, "tld", "com"));
                indexWriter.addDocument(createEnrichDocument("elastic.co", "globalRank", 451, "tldRank",23, "tld", "co"));
                indexWriter.addDocument(createEnrichDocument("bbc.co.uk", "globalRank", 45, "tldRank", 14, "tld", "co.uk"));
                indexWriter.addDocument(createEnrichDocument("eops.nl", "globalRank", 4567, "tldRank", 80, "tld", "nl"));
                indexWriter.commit();

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "elastic.com"));
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
                indexWriter.addDocument(createEnrichDocument("elastic.co", "globalRank", 451, "tldRank",23, "tld", "co"));
                indexWriter.commit();
                indexWriter.addDocument(createEnrichDocument("eops.nl", "globalRank", 4567, "tldRank", 80, "tld", "nl"));
                indexWriter.commit();

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "elastic.co"));
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

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "elastic.co"));
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

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    ExactMatchProcessor processor =
                        new ExactMatchProcessor("_tag", createSearchProvider(indexReader), "_name", "domain", false,
                            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

                    IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
                        Collections.singletonMap("domain", "elastic.co"));
                    Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
                    assertThat(e.getMessage(), equalTo("enrich key field does not exist"));
                }
            }
        }
    }

    public void testIndexMetadataMissing() {
        Function<String, Tuple<IndexMetaData, Engine.Searcher>> provider = indexExpression -> new Tuple<>(null, null);
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", provider, "_name", "domain", false,
            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.co"));
        expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
    }

    public void testMetaFieldMissing() throws Exception {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetaData imd = IndexMetaData.builder("majestic_index")
            .settings(indexSettings)
            .putMapping("_doc", "{}")
            .build();

        Function<String, Tuple<IndexMetaData, Engine.Searcher>> provider = indexExpression -> new Tuple<>(imd, null);
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", provider, "_name", "domain", false,
            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.co"));
        expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
    }

    public void testEnrichKeyFieldNameMissing() throws Exception {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetaData imd = IndexMetaData.builder("majestic_index")
            .settings(indexSettings)
            .putMapping("_doc", "{\"_meta\": {}}")
            .build();

        Function<String, Tuple<IndexMetaData, Engine.Searcher>> provider = indexExpression -> new Tuple<>(imd, null);
        ExactMatchProcessor processor = new ExactMatchProcessor("_tag", provider, "_name", "domain", false,
            Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));

        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.co"));
        expectThrows(IllegalStateException.class, () -> processor.execute(ingestDocument));
    }

    public void testIgnoreKeyMissing() throws Exception {
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", ndexExpression -> null, "_name", "domain",
                true, Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument =
                    new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Collections.emptyMap());

            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
            assertThat(processor.execute(ingestDocument), notNullValue());
            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
        }
        {
            ExactMatchProcessor processor = new ExactMatchProcessor("_tag", indexExpression -> null, "_name", "domain",
                false, Arrays.asList(new EnrichSpecification("tldRank", "tld_rank"), new EnrichSpecification("tld", "tld")));
            IngestDocument ingestDocument =
                    new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, Collections.emptyMap());
            expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        }
    }

    private static Document createEnrichDocument(String key, Object... decorateValues) throws IOException {
        assert decorateValues.length % 2 ==0;

        BytesReference decorateContent;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < decorateValues.length; i += 2) {
                map.put((String) decorateValues[i], decorateValues[i + 1]);
            }
            builder.map(map);
            builder.flush();
            ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
            decorateContent = new BytesArray(outputStream.toByteArray());
        }
        Document document = new Document();
        document.add(new StringField("key", key, Field.Store.NO));
        document.add(new StoredField(SourceFieldMapper.NAME, decorateContent.toBytesRef()));
        return document;
    }

    private static Function<String, Tuple<IndexMetaData, Engine.Searcher>> createSearchProvider(IndexReader indexReader) throws Exception {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetaData imd = IndexMetaData.builder("majestic_index")
            .settings(indexSettings)
            .putMapping("_doc", "{\"_meta\": {\"" + ENRICH_KEY_FIELD_NAME +"\": \"key\"}}")
            .build();

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        Engine.Searcher searcher = new Engine.Searcher("_enrich", indexSearcher, indexReader);
        return indexExpression -> new Tuple<>(imd, searcher);
    }
}
