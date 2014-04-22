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

package org.elasticsearch.action.termvector;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class GetTermVectorCheckDocFreqTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put(super.indexSettings())
                .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")
                .build();
    }

    @Test
    public void testSimpleTermVectors() throws ElasticsearchException, IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                            .field("analyzer", "tv_test")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();
        int numDocs = 15;
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                    // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                    // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        String[] values = { "brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the" };
        int[] freq = { 1, 1, 1, 1, 1, 1, 1, 2 };
        int[][] pos = { { 2 }, { 8 }, { 3 }, { 4 }, { 7 }, { 5 }, { 1 }, { 0, 6 } };
        int[][] startOffset = { { 10 }, { 40 }, { 16 }, { 20 }, { 35 }, { 26 }, { 4 }, { 0, 31 } };
        int[][] endOffset = { { 15 }, { 43 }, { 19 }, { 25 }, { 39 }, { 30 }, { 9 }, { 3, 34 } };
        for (int i = 0; i < numDocs; i++) {
            checkAllInfo(numDocs, values, freq, pos, startOffset, endOffset, i);
            checkWithoutTermStatistics(numDocs, values, freq, pos, startOffset, endOffset, i);
            checkWithoutFieldStatistics(numDocs, values, freq, pos, startOffset, endOffset, i);
        }
    }

    private void checkWithoutFieldStatistics(int numDocs, String[] values, int[] freq, int[][] pos, int[][] startOffset, int[][] endOffset,
            int i) throws IOException {
        TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i)).setPayloads(true).setOffsets(true)
                .setPositions(true).setTermStatistics(true).setFieldStatistics(false).setSelectedFields();
        TermVectorResponse response = resp.execute().actionGet();
        assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        assertThat(terms.size(), equalTo(8l));
        assertThat(terms.getSumTotalTermFreq(), Matchers.equalTo((long) -1));
        assertThat(terms.getDocCount(), Matchers.equalTo(-1));
        assertThat(terms.getSumDocFreq(), equalTo((long) -1));
        TermsEnum iterator = terms.iterator(null);
        for (int j = 0; j < values.length; j++) {
            String string = values[j];
            BytesRef next = iterator.next();
            assertThat(next, Matchers.notNullValue());
            assertThat("expected " + string, string, equalTo(next.utf8ToString()));
            assertThat(next, Matchers.notNullValue());
            if (string.equals("the")) {
                assertThat("expected ttf of " + string, numDocs * 2, equalTo((int) iterator.totalTermFreq()));
            } else {
                assertThat("expected ttf of " + string, numDocs, equalTo((int) iterator.totalTermFreq()));
            }

            DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            assertThat(freq[j], equalTo(docsAndPositions.freq()));
            assertThat(iterator.docFreq(), equalTo(numDocs));
            int[] termPos = pos[j];
            int[] termStartOffset = startOffset[j];
            int[] termEndOffset = endOffset[j];
            assertThat(termPos.length, equalTo(freq[j]));
            assertThat(termStartOffset.length, equalTo(freq[j]));
            assertThat(termEndOffset.length, equalTo(freq[j]));
            for (int k = 0; k < freq[j]; k++) {
                int nextPosition = docsAndPositions.nextPosition();
                assertThat("term: " + string, nextPosition, equalTo(termPos[k]));
                assertThat("term: " + string, docsAndPositions.startOffset(), equalTo(termStartOffset[k]));
                assertThat("term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
            }
        }
        assertThat(iterator.next(), Matchers.nullValue());

        XContentBuilder xBuilder = XContentFactory.jsonBuilder();
        xBuilder.startObject();
        response.toXContent(xBuilder, null);
        xBuilder.endObject();
        BytesStream bytesStream = xBuilder.bytesStream();
        String utf8 = bytesStream.bytes().toUtf8();
        String expectedString = "{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\""
                + i
                + "\",\"_version\":1,\"found\":true,\"term_vectors\":{\"field\":{\"terms\":{\"brown\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":2,\"start_offset\":10,\"end_offset\":15,\"payload\":\"d29yZA==\"}]},\"dog\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":8,\"start_offset\":40,\"end_offset\":43,\"payload\":\"d29yZA==\"}]},\"fox\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":3,\"start_offset\":16,\"end_offset\":19,\"payload\":\"d29yZA==\"}]},\"jumps\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":4,\"start_offset\":20,\"end_offset\":25,\"payload\":\"d29yZA==\"}]},\"lazy\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":7,\"start_offset\":35,\"end_offset\":39,\"payload\":\"d29yZA==\"}]},\"over\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":5,\"start_offset\":26,\"end_offset\":30,\"payload\":\"d29yZA==\"}]},\"quick\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":1,\"start_offset\":4,\"end_offset\":9,\"payload\":\"d29yZA==\"}]},\"the\":{\"doc_freq\":15,\"ttf\":30,\"term_freq\":2,\"tokens\":[{\"position\":0,\"start_offset\":0,\"end_offset\":3,\"payload\":\"d29yZA==\"},{\"position\":6,\"start_offset\":31,\"end_offset\":34,\"payload\":\"d29yZA==\"}]}}}}}";
        assertThat(utf8, equalTo(expectedString));

    }

    private void checkWithoutTermStatistics(int numDocs, String[] values, int[] freq, int[][] pos, int[][] startOffset, int[][] endOffset,
            int i) throws IOException {
        TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i)).setPayloads(true).setOffsets(true)
                .setPositions(true).setTermStatistics(false).setFieldStatistics(true).setSelectedFields();
        assertThat(resp.request().termStatistics(), equalTo(false));
        TermVectorResponse response = resp.execute().actionGet();
        assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        assertThat(terms.size(), equalTo(8l));
        assertThat(terms.getSumTotalTermFreq(), Matchers.equalTo((long) (9 * numDocs)));
        assertThat(terms.getDocCount(), Matchers.equalTo(numDocs));
        assertThat(terms.getSumDocFreq(), equalTo((long) numDocs * values.length));
        TermsEnum iterator = terms.iterator(null);
        for (int j = 0; j < values.length; j++) {
            String string = values[j];
            BytesRef next = iterator.next();
            assertThat(next, Matchers.notNullValue());
            assertThat("expected " + string, string, equalTo(next.utf8ToString()));
            assertThat(next, Matchers.notNullValue());

            assertThat("expected ttf of " + string, -1, equalTo((int) iterator.totalTermFreq()));

            DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            assertThat(freq[j], equalTo(docsAndPositions.freq()));
            assertThat(iterator.docFreq(), equalTo(-1));
            int[] termPos = pos[j];
            int[] termStartOffset = startOffset[j];
            int[] termEndOffset = endOffset[j];
            assertThat(termPos.length, equalTo(freq[j]));
            assertThat(termStartOffset.length, equalTo(freq[j]));
            assertThat(termEndOffset.length, equalTo(freq[j]));
            for (int k = 0; k < freq[j]; k++) {
                int nextPosition = docsAndPositions.nextPosition();
                assertThat("term: " + string, nextPosition, equalTo(termPos[k]));
                assertThat("term: " + string, docsAndPositions.startOffset(), equalTo(termStartOffset[k]));
                assertThat("term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
            }
        }
        assertThat(iterator.next(), Matchers.nullValue());

        XContentBuilder xBuilder = XContentFactory.jsonBuilder();
        xBuilder.startObject();
        response.toXContent(xBuilder, null);
        xBuilder.endObject();
        BytesStream bytesStream = xBuilder.bytesStream();
        String utf8 = bytesStream.bytes().toUtf8();
        String expectedString = "{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\""
                + i
                + "\",\"_version\":1,\"found\":true,\"term_vectors\":{\"field\":{\"field_statistics\":{\"sum_doc_freq\":120,\"doc_count\":15,\"sum_ttf\":135},\"terms\":{\"brown\":{\"term_freq\":1,\"tokens\":[{\"position\":2,\"start_offset\":10,\"end_offset\":15,\"payload\":\"d29yZA==\"}]},\"dog\":{\"term_freq\":1,\"tokens\":[{\"position\":8,\"start_offset\":40,\"end_offset\":43,\"payload\":\"d29yZA==\"}]},\"fox\":{\"term_freq\":1,\"tokens\":[{\"position\":3,\"start_offset\":16,\"end_offset\":19,\"payload\":\"d29yZA==\"}]},\"jumps\":{\"term_freq\":1,\"tokens\":[{\"position\":4,\"start_offset\":20,\"end_offset\":25,\"payload\":\"d29yZA==\"}]},\"lazy\":{\"term_freq\":1,\"tokens\":[{\"position\":7,\"start_offset\":35,\"end_offset\":39,\"payload\":\"d29yZA==\"}]},\"over\":{\"term_freq\":1,\"tokens\":[{\"position\":5,\"start_offset\":26,\"end_offset\":30,\"payload\":\"d29yZA==\"}]},\"quick\":{\"term_freq\":1,\"tokens\":[{\"position\":1,\"start_offset\":4,\"end_offset\":9,\"payload\":\"d29yZA==\"}]},\"the\":{\"term_freq\":2,\"tokens\":[{\"position\":0,\"start_offset\":0,\"end_offset\":3,\"payload\":\"d29yZA==\"},{\"position\":6,\"start_offset\":31,\"end_offset\":34,\"payload\":\"d29yZA==\"}]}}}}}";
        assertThat(utf8, equalTo(expectedString));

    }

    private void checkAllInfo(int numDocs, String[] values, int[] freq, int[][] pos, int[][] startOffset, int[][] endOffset, int i)
            throws IOException {
        TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i)).setPayloads(true).setOffsets(true)
                .setPositions(true).setFieldStatistics(true).setTermStatistics(true).setSelectedFields();
        assertThat(resp.request().fieldStatistics(), equalTo(true));
        TermVectorResponse response = resp.execute().actionGet();
        assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        assertThat(terms.size(), equalTo(8l));
        assertThat(terms.getSumTotalTermFreq(), Matchers.equalTo((long) (9 * numDocs)));
        assertThat(terms.getDocCount(), Matchers.equalTo(numDocs));
        assertThat(terms.getSumDocFreq(), equalTo((long) numDocs * values.length));
        TermsEnum iterator = terms.iterator(null);
        for (int j = 0; j < values.length; j++) {
            String string = values[j];
            BytesRef next = iterator.next();
            assertThat(next, Matchers.notNullValue());
            assertThat("expected " + string, string, equalTo(next.utf8ToString()));
            assertThat(next, Matchers.notNullValue());
            if (string.equals("the")) {
                assertThat("expected ttf of " + string, numDocs * 2, equalTo((int) iterator.totalTermFreq()));
            } else {
                assertThat("expected ttf of " + string, numDocs, equalTo((int) iterator.totalTermFreq()));
            }

            DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            assertThat(freq[j], equalTo(docsAndPositions.freq()));
            assertThat(iterator.docFreq(), equalTo(numDocs));
            int[] termPos = pos[j];
            int[] termStartOffset = startOffset[j];
            int[] termEndOffset = endOffset[j];
            assertThat(termPos.length, equalTo(freq[j]));
            assertThat(termStartOffset.length, equalTo(freq[j]));
            assertThat(termEndOffset.length, equalTo(freq[j]));
            for (int k = 0; k < freq[j]; k++) {
                int nextPosition = docsAndPositions.nextPosition();
                assertThat("term: " + string, nextPosition, equalTo(termPos[k]));
                assertThat("term: " + string, docsAndPositions.startOffset(), equalTo(termStartOffset[k]));
                assertThat("term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
            }
        }
        assertThat(iterator.next(), Matchers.nullValue());

        XContentBuilder xBuilder = XContentFactory.jsonBuilder();
        xBuilder.startObject();
        response.toXContent(xBuilder, ToXContent.EMPTY_PARAMS);
        xBuilder.endObject();
        BytesStream bytesStream = xBuilder.bytesStream();
        String utf8 = bytesStream.bytes().toUtf8();
        String expectedString = "{\"_index\":\"test\",\"_type\":\"type1\",\"_id\":\""
                + i
                + "\",\"_version\":1,\"found\":true,\"term_vectors\":{\"field\":{\"field_statistics\":{\"sum_doc_freq\":120,\"doc_count\":15,\"sum_ttf\":135},\"terms\":{\"brown\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":2,\"start_offset\":10,\"end_offset\":15,\"payload\":\"d29yZA==\"}]},\"dog\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":8,\"start_offset\":40,\"end_offset\":43,\"payload\":\"d29yZA==\"}]},\"fox\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":3,\"start_offset\":16,\"end_offset\":19,\"payload\":\"d29yZA==\"}]},\"jumps\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":4,\"start_offset\":20,\"end_offset\":25,\"payload\":\"d29yZA==\"}]},\"lazy\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":7,\"start_offset\":35,\"end_offset\":39,\"payload\":\"d29yZA==\"}]},\"over\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":5,\"start_offset\":26,\"end_offset\":30,\"payload\":\"d29yZA==\"}]},\"quick\":{\"doc_freq\":15,\"ttf\":15,\"term_freq\":1,\"tokens\":[{\"position\":1,\"start_offset\":4,\"end_offset\":9,\"payload\":\"d29yZA==\"}]},\"the\":{\"doc_freq\":15,\"ttf\":30,\"term_freq\":2,\"tokens\":[{\"position\":0,\"start_offset\":0,\"end_offset\":3,\"payload\":\"d29yZA==\"},{\"position\":6,\"start_offset\":31,\"end_offset\":34,\"payload\":\"d29yZA==\"}]}}}}}";
        assertThat(utf8, equalTo(expectedString));
    }

}
