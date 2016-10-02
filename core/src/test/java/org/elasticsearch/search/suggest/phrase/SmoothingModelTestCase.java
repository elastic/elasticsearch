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
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

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

    protected abstract SmoothingModel fromXContent(QueryParseContext context) throws IOException;

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
        XContentParser parser = XContentHelper.createParser(shuffleXContent(contentBuilder).bytes());
        QueryParseContext context = new QueryParseContext(new IndicesQueriesRegistry(), parser, ParseFieldMatcher.STRICT);
        parser.nextToken();  // go to start token, real parsing would do that in the outer element parser
        SmoothingModel parsedModel = fromXContent(context);
        assertNotSame(testModel, parsedModel);
        assertEquals(testModel, parsedModel);
        assertEquals(testModel.hashCode(), parsedModel.hashCode());
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

        WordScorer wordScorer = testModel.buildWordScorerFactory().newScorer(ir, MultiFields.getTerms(ir, "field"), "field", 0.9d,
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
        SmoothingModel deserializedModel = copyModel(testModel);
        assertEquals(testModel, deserializedModel);
        assertEquals(testModel.hashCode(), deserializedModel.hashCode());
        assertNotSame(testModel, deserializedModel);
    }

    /**
     * Test equality and hashCode properties
     */
    @SuppressWarnings("unchecked")
    public void testEqualsAndHashcode() throws IOException {
        SmoothingModel firstModel = createTestModel();
        assertFalse("smoothing model is equal to null", firstModel.equals(null));
        assertFalse("smoothing model is equal to incompatible type", firstModel.equals(""));
        assertTrue("smoothing model is not equal to self", firstModel.equals(firstModel));
        assertThat("same smoothing model's hashcode returns different values if called multiple times", firstModel.hashCode(),
                equalTo(firstModel.hashCode()));
        assertThat("different smoothing models should not be equal", createMutation(firstModel), not(equalTo(firstModel)));

        SmoothingModel secondModel = copyModel(firstModel);
        assertTrue("smoothing model is not equal to self", secondModel.equals(secondModel));
        assertTrue("smoothing model is not equal to its copy", firstModel.equals(secondModel));
        assertTrue("equals is not symmetric", secondModel.equals(firstModel));
        assertThat("smoothing model copy's hashcode is different from original hashcode", secondModel.hashCode(),
                equalTo(firstModel.hashCode()));

        SmoothingModel thirdModel = copyModel(secondModel);
        assertTrue("smoothing model is not equal to self", thirdModel.equals(thirdModel));
        assertTrue("smoothing model is not equal to its copy", secondModel.equals(thirdModel));
        assertThat("smoothing model copy's hashcode is different from original hashcode", secondModel.hashCode(),
                equalTo(thirdModel.hashCode()));
        assertTrue("equals is not transitive", firstModel.equals(thirdModel));
        assertThat("smoothing model copy's hashcode is different from original hashcode", firstModel.hashCode(),
                equalTo(thirdModel.hashCode()));
        assertTrue("equals is not symmetric", thirdModel.equals(secondModel));
        assertTrue("equals is not symmetric", thirdModel.equals(firstModel));
    }

    static SmoothingModel copyModel(SmoothingModel original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return namedWriteableRegistry.getReader(SmoothingModel.class, original.getWriteableName()).read(in);
            }
        }
    }

}
