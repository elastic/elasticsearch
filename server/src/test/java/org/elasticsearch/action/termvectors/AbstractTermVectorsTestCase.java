/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTermVectorsTestCase extends ESIntegTestCase {
    protected record TestFieldSetting(String name, boolean storedOffset, boolean storedPayloads, boolean storedPositions) {

        public void addToMappings(XContentBuilder mappingsBuilder) throws IOException {
            mappingsBuilder.startObject(name);
            mappingsBuilder.field("type", "text");
            String tv_settings;
            if (storedPositions && storedOffset && storedPayloads) {
                tv_settings = "with_positions_offsets_payloads";
            } else if (storedPositions && storedOffset) {
                tv_settings = "with_positions_offsets";
            } else if (storedPayloads) {
                tv_settings = "with_positions_payloads";
            } else if (storedPositions) {
                tv_settings = "with_positions";
            } else if (storedOffset) {
                tv_settings = "with_offsets";
            } else {
                tv_settings = "yes";
            }

            mappingsBuilder.field("term_vector", tv_settings);

            if (storedPayloads) {
                mappingsBuilder.field("analyzer", "tv_test");
            }

            mappingsBuilder.endObject();
        }
    }

    protected static class TestDoc {
        public final String id;
        public final TestFieldSetting[] fieldSettings;
        public final String[] fieldContent;
        public String index = "test";
        public String alias = "alias";

        public TestDoc(String id, TestFieldSetting[] fieldSettings, String[] fieldContent) {
            this.id = id;
            assertEquals(fieldSettings.length, fieldContent.length);
            this.fieldSettings = fieldSettings;
            this.fieldContent = fieldContent;
        }

        public TestDoc index(String index) {
            this.index = index;
            return this;
        }

        public TestDoc alias(String alias) {
            this.alias = alias;
            return this;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder("index:").append(index).append(" id:").append(id);
            for (int i = 0; i < fieldSettings.length; i++) {
                TestFieldSetting f = fieldSettings[i];
                sb.append("\n").append("Field: ").append(f).append("\n  content:").append(fieldContent[i]);
            }
            sb.append("\n");

            return sb.toString();
        }
    }

    protected static class TestConfig {
        public final TestDoc doc;
        public final String[] selectedFields;
        public final boolean requestPositions;
        public final boolean requestOffsets;
        public final boolean requestPayloads;
        public Class<? extends Exception> expectedException = null;

        public TestConfig(TestDoc doc, String[] selectedFields, boolean requestPositions, boolean requestOffsets, boolean requestPayloads) {
            this.doc = doc;
            this.selectedFields = selectedFields;
            this.requestPositions = requestPositions;
            this.requestOffsets = requestOffsets;
            this.requestPayloads = requestPayloads;
        }

        public TestConfig expectedException(Class<? extends Exception> exceptionClass) {
            this.expectedException = exceptionClass;
            return this;
        }

        @Override
        public String toString() {
            String requested = "";
            if (requestOffsets) {
                requested += "offsets,";
            }
            if (requestPositions) {
                requested += "position,";
            }
            if (requestPayloads) {
                requested += "payload,";
            }
            Locale aLocale = new Locale("en", "US");
            return String.format(
                aLocale,
                "(doc: %s\n requested: %s, fields: %s)",
                doc,
                requested,
                selectedFields == null ? "NULL" : String.join(",", selectedFields)
            );
        }
    }

    protected void createIndexBasedOnFieldSettings(String index, String alias, TestFieldSetting[] fieldSettings) throws IOException {
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject().startObject("_doc").startObject("properties");
        for (TestFieldSetting field : fieldSettings) {
            field.addToMappings(mappingBuilder);
        }
        mappingBuilder.endObject().endObject().endObject();
        Settings.Builder settings = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
            .putList("index.analysis.analyzer.tv_test.filter", "lowercase");
        assertAcked(prepareCreate(index).setMapping(mappingBuilder).setSettings(settings).addAlias(new Alias(alias)));
    }

    /**
     * Generate test documentsThe returned documents are already indexed.
     */
    protected TestDoc[] generateTestDocs(String index, TestFieldSetting[] fieldSettings) {
        String[] fieldContentOptions = new String[] {
            "Generating a random permutation of a sequence (such as when shuffling cards).",
            "Selecting a random sample of a population (important in statistical sampling).",
            "Allocating experimental units via random assignment to a treatment or control condition.",
            "Generating random numbers: see Random number generation.",
            "Transforming a data stream (such as when using a scrambler in telecommunications)." };

        String[] contentArray = new String[fieldSettings.length];
        Map<String, Object> docSource = new HashMap<>();
        int totalShards = getNumShards(index).numPrimaries;
        TestDoc[] testDocs = new TestDoc[totalShards];
        // this methods wants to send one doc to each shard
        for (int i = 0; i < totalShards; i++) {
            docSource.clear();
            for (int j = 0; j < contentArray.length; j++) {
                contentArray[j] = fieldContentOptions[randomInt(fieldContentOptions.length - 1)];
                docSource.put(fieldSettings[j].name, contentArray[j]);
            }
            final String id = routingKeyForShard(index, i);
            TestDoc doc = new TestDoc(id, fieldSettings, contentArray.clone());
            index(doc.index, doc.id, docSource);
            testDocs[i] = doc;
        }

        refresh();
        return testDocs;

    }

    protected TestConfig[] generateTestConfigs(int numberOfTests, TestDoc[] testDocs, TestFieldSetting[] fieldSettings) {
        ArrayList<TestConfig> configs = new ArrayList<>();
        for (int i = 0; i < numberOfTests; i++) {

            ArrayList<String> selectedFields = null;
            if (randomBoolean()) {
                // used field selection
                selectedFields = new ArrayList<>();
                if (randomBoolean()) {
                    selectedFields.add("Doesnt_exist"); // this will be ignored.
                }
                for (TestFieldSetting field : fieldSettings) {
                    if (randomBoolean()) {
                        selectedFields.add(field.name);
                    }
                }

                if (selectedFields.size() == 0) {
                    selectedFields = null; // 0 length set is not supported.
                }

            }
            TestConfig config = new TestConfig(
                testDocs[randomInt(testDocs.length - 1)],
                selectedFields == null ? null : selectedFields.toArray(new String[] {}),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );

            configs.add(config);
        }
        // always adds a test that fails
        configs.add(
            new TestConfig(
                new TestDoc("doesnt_exist", new TestFieldSetting[] {}, new String[] {}).index("doesn't_exist").alias("doesn't_exist"),
                new String[] { "doesnt_exist" },
                true,
                true,
                true
            ).expectedException(org.elasticsearch.index.IndexNotFoundException.class)
        );

        refresh();

        return configs.toArray(new TestConfig[configs.size()]);
    }

    protected TestFieldSetting[] getFieldSettings() {
        return new TestFieldSetting[] {
            new TestFieldSetting("field_with_positions", false, false, true),
            new TestFieldSetting("field_with_offsets", true, false, false),
            new TestFieldSetting("field_with_only_tv", false, false, false),
            new TestFieldSetting("field_with_positions_offsets", false, false, true),
            new TestFieldSetting("field_with_positions_payloads", false, true, true) };
    }

    protected DirectoryReader indexDocsWithLucene(TestDoc[] testDocs) throws IOException {
        Map<String, Analyzer> mapping = new HashMap<>();
        for (TestFieldSetting field : testDocs[0].fieldSettings) {
            if (field.storedPayloads) {
                mapping.put(field.name, new Analyzer() {
                    @Override
                    protected TokenStreamComponents createComponents(String fieldName) {
                        Tokenizer tokenizer = new StandardTokenizer();
                        TokenFilter filter = new LowerCaseFilter(tokenizer);
                        filter = new TypeAsPayloadTokenFilter(filter);
                        return new TokenStreamComponents(tokenizer, filter);
                    }

                });
            }
        }
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(CharArraySet.EMPTY_SET), mapping);

        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(wrapper);

        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);

        for (TestDoc doc : testDocs) {
            Document d = new Document();
            d.add(new Field("id", doc.id, StringField.TYPE_STORED));
            for (int i = 0; i < doc.fieldContent.length; i++) {
                FieldType type = new FieldType(TextField.TYPE_STORED);
                TestFieldSetting fieldSetting = doc.fieldSettings[i];

                type.setStoreTermVectorOffsets(fieldSetting.storedOffset);
                type.setStoreTermVectorPayloads(fieldSetting.storedPayloads);
                type.setStoreTermVectorPositions(fieldSetting.storedPositions || fieldSetting.storedPayloads || fieldSetting.storedOffset);
                type.setStoreTermVectors(true);
                type.freeze();
                d.add(new Field(fieldSetting.name, doc.fieldContent[i], type));
            }
            writer.updateDocument(new Term("id", doc.id), d);
            writer.commit();
        }
        writer.close();

        return DirectoryReader.open(dir);
    }

    protected void validateResponse(TermVectorsResponse esResponse, Fields luceneFields, TestConfig testConfig) throws IOException {
        assertThat(esResponse.getIndex(), equalTo(testConfig.doc.index));
        TestDoc testDoc = testConfig.doc;
        HashSet<String> selectedFields = testConfig.selectedFields == null ? null : new HashSet<>(Arrays.asList(testConfig.selectedFields));
        Fields esTermVectorFields = esResponse.getFields();
        for (TestFieldSetting field : testDoc.fieldSettings) {
            Terms esTerms = esTermVectorFields.terms(field.name);
            if (selectedFields != null && selectedFields.contains(field.name) == false) {
                if (esTerms != null) {
                    fail("Expecting only terms for fields " + selectedFields + " but got " + field.name);
                }
                continue;
            }

            assertNotNull(esTerms);

            Terms luceneTerms = luceneFields.terms(field.name);
            TermsEnum esTermEnum = esTerms.iterator();
            TermsEnum luceneTermEnum = luceneTerms.iterator();

            while (esTermEnum.next() != null) {
                assertNotNull(luceneTermEnum.next());

                assertThat(esTermEnum.totalTermFreq(), equalTo(luceneTermEnum.totalTermFreq()));
                PostingsEnum esDocsPosEnum = esTermEnum.postings(null, PostingsEnum.POSITIONS);
                PostingsEnum luceneDocsPosEnum = luceneTermEnum.postings(null, PostingsEnum.POSITIONS);
                if (luceneDocsPosEnum == null) {
                    // test we expect that...
                    assertFalse(field.storedOffset);
                    assertFalse(field.storedPayloads);
                    assertFalse(field.storedPositions);
                    continue;
                }

                String currentTerm = esTermEnum.term().utf8ToString();

                assertThat("Token mismatch for field: " + field.name, currentTerm, equalTo(luceneTermEnum.term().utf8ToString()));

                esDocsPosEnum.nextDoc();
                luceneDocsPosEnum.nextDoc();

                int freq = esDocsPosEnum.freq();
                assertThat(freq, equalTo(luceneDocsPosEnum.freq()));
                for (int i = 0; i < freq; i++) {
                    String failDesc = " (field:" + field.name + " term:" + currentTerm + ")";
                    int lucenePos = luceneDocsPosEnum.nextPosition();
                    int esPos = esDocsPosEnum.nextPosition();
                    if (field.storedPositions && testConfig.requestPositions) {
                        assertThat("Position test failed" + failDesc, lucenePos, equalTo(esPos));
                    } else {
                        assertThat("Missing position test failed" + failDesc, esPos, equalTo(-1));
                    }
                    if (field.storedOffset && testConfig.requestOffsets) {
                        assertThat("Offset test failed" + failDesc, luceneDocsPosEnum.startOffset(), equalTo(esDocsPosEnum.startOffset()));
                        assertThat("Offset test failed" + failDesc, luceneDocsPosEnum.endOffset(), equalTo(esDocsPosEnum.endOffset()));
                    } else {
                        assertThat("Missing offset test failed" + failDesc, esDocsPosEnum.startOffset(), equalTo(-1));
                        assertThat("Missing offset test failed" + failDesc, esDocsPosEnum.endOffset(), equalTo(-1));
                    }
                    assertNull("Missing payload test failed" + failDesc, esDocsPosEnum.getPayload());
                }
            }
            assertNull("Es returned terms are done but lucene isn't", luceneTermEnum.next());
        }
    }

    protected TermVectorsRequestBuilder getRequestForConfig(TestConfig config) {
        return client().prepareTermVectors(randomBoolean() ? config.doc.index : config.doc.alias, config.doc.id)
            .setPayloads(config.requestPayloads)
            .setOffsets(config.requestOffsets)
            .setPositions(config.requestPositions)
            .setFieldStatistics(true)
            .setTermStatistics(true)
            .setSelectedFields(config.selectedFields)
            .setRealtime(false);
    }

    protected Fields getTermVectorsFromLucene(DirectoryReader directoryReader, TestDoc doc) throws IOException {
        IndexSearcher searcher = newSearcher(directoryReader);
        TopDocs search = searcher.search(new TermQuery(new Term("id", doc.id)), 1);

        ScoreDoc[] scoreDocs = search.scoreDocs;
        assertEquals(1, scoreDocs.length);
        return directoryReader.getTermVectors(scoreDocs[0].doc);
    }
}
