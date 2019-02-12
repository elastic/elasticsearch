/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class SmoothingModelTestCase extends ESTestCase {

    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        if (namedWriteableRegistry == null) {
            List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
            SearchModule.registerSmoothingModels(namedWriteables);
            namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * create random model that is put under test
     */
    protected abstract SmoothingModel createTestModel();

    /**
     * mutate the given model so the returned smoothing model is different
     */
    protected abstract SmoothingModel createMutation(SmoothingModel original) throws IOException;

    protected abstract SmoothingModel fromXContent(XContentParser parser) throws IOException;

    /**
     * Test that creates new smoothing model from a random test smoothing model and checks both for equality
     */
    public void testFromXContent() throws IOException {
        SmoothingModel testModel = createTestModel();
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            contentBuilder.prettyPrint();
        }
        contentBuilder.startObject();
        testModel.innerToXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        try (XContentParser parser = createParser(shuffleXContent(contentBuilder))) {
            parser.nextToken();  // go to start token, real parsing would do that in the outer element parser
            SmoothingModel parsedModel = fromXContent(parser);
            assertNotSame(testModel, parsedModel);
            assertEquals(testModel, parsedModel);
            assertEquals(testModel.hashCode(), parsedModel.hashCode());
        }
    }

    /**
     * Test the WordScorer emitted by the smoothing model
     */
    public void testBuildWordScorer() throws IOException {
        SmoothingModel testModel = createTestModel();
        Map<String, Analyzer> mapping = new HashMap<>();
        mapping.put("field", new WhitespaceAnalyzer());
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), mapping);
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(wrapper));
        Document doc = new Document();
        doc.add(new Field("field", "someText", TextField.TYPE_NOT_STORED));
        writer.addDocument(doc);
        DirectoryReader ir = DirectoryReader.open(writer);

        WordScorer wordScorer = testModel.buildWordScorerFactory().newScorer(ir, MultiTerms.getTerms(ir, "field"), "field", 0.9d,
                BytesRefs.toBytesRef(" "));
        assertWordScorer(wordScorer, testModel);
    }

    /**
     * implementation dependant assertions on the wordScorer produced by the smoothing model under test
     */
    abstract void assertWordScorer(WordScorer wordScorer, SmoothingModel testModel);

    /**
     * Test serialization and deserialization of the tested model.
     */
    public void testSerialization() throws IOException {
        SmoothingModel testModel = createTestModel();
        SmoothingModel deserializedModel = copy(testModel);
        assertEquals(testModel, deserializedModel);
        assertEquals(testModel.hashCode(), deserializedModel.hashCode());
        assertNotSame(testModel, deserializedModel);
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        checkEqualsAndHashCode(createTestModel(), this::copy, this::createMutation);
    }

    private SmoothingModel copy(SmoothingModel original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWriteableRegistry,
                namedWriteableRegistry.getReader(SmoothingModel.class, original.getWriteableName()));
    }
}
