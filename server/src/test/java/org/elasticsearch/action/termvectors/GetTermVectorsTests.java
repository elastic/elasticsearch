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
package org.elasticsearch.action.termvectors;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetTermVectorsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(MockPayloadAnalyzerPlugin.class);
    }

    // Delimited payload token filter was moved to analysis-common module,
    // This test relies heavily on this token filter, even though it is not testing this token filter.
    // Solution for now is copy what delimited payload token filter does in this test.
    // Unfortunately MockPayloadAnalyzer couldn't be used here as it misses functionality.
    public static class MockPayloadAnalyzerPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("mock_payload_filter", (indexSettings, environment, name, settings) -> {
                return new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return "mock_payload_filter";
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        String delimiter = settings.get("delimiter");
                        PayloadEncoder encoder = null;
                        if (settings.get("encoding").equals("float")) {
                            encoder = new FloatEncoder();
                        } else if (settings.get("encoding").equals("int")) {
                            encoder = new IntegerEncoder();
                        } else if (settings.get("encoding").equals("identity")) {
                            encoder = new IdentityEncoder();
                        }
                        return new MockPayloadTokenFilter(tokenStream, delimiter.charAt(0), encoder);
                    }
                };
            });
        }

        @Override
        public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
            return Collections.singletonList(PreConfiguredTokenizer.singleton("mock-whitespace",
                () -> new MockTokenizer(MockTokenizer.WHITESPACE, false)));
        }

        // Based on DelimitedPayloadTokenFilter:
        final class MockPayloadTokenFilter extends TokenFilter {
            private final char delimiter;
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);
            private final PayloadEncoder encoder;


            MockPayloadTokenFilter(TokenStream input, char delimiter, PayloadEncoder encoder) {
                super(input);
                this.delimiter = delimiter;
                this.encoder = encoder;
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (input.incrementToken()) {
                    final char[] buffer = termAtt.buffer();
                    final int length = termAtt.length();
                    for (int i = 0; i < length; i++) {
                        if (buffer[i] == delimiter) {
                            payAtt.setPayload(encoder.encode(buffer, i + 1, (length - (i + 1))));
                            termAtt.setLength(i); // simply set a new length
                            return true;
                        }
                    }
                    // we have not seen the delimiter
                    payAtt.setPayload(null);
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    public void testRandomPayloadWithDelimitedPayloadTokenFilter() throws IOException {
        //create the test document
        int encoding = randomIntBetween(0, 2);
        String encodingString = "";
        if (encoding == 0) {
            encodingString = "float";
        }
        if (encoding == 1) {
            encodingString = "int";
        }
        if (encoding == 2) {
            encodingString = "identity";
        }
        String[] tokens = crateRandomTokens();
        Map<String, List<BytesRef>> payloads = createPayloads(tokens, encoding);
        String delimiter = createRandomDelimiter(tokens);
        String queryString = createString(tokens, payloads, encoding, delimiter.charAt(0));
        //create the mapping
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties")
                .startObject("field").field("type", "text").field("term_vector", "with_positions_offsets_payloads")
                .field("analyzer", "payload_test").endObject().endObject().endObject().endObject();
        Settings setting =  Settings.builder()
            .put("index.analysis.analyzer.payload_test.tokenizer", "mock-whitespace")
            .putList("index.analysis.analyzer.payload_test.filter", "my_delimited_payload")
            .put("index.analysis.filter.my_delimited_payload.delimiter", delimiter)
            .put("index.analysis.filter.my_delimited_payload.encoding", encodingString)
            .put("index.analysis.filter.my_delimited_payload.type", "mock_payload_filter").build();
        createIndex("test", setting, mapping);

        client().prepareIndex("test").setId(Integer.toString(1))
                .setSource(jsonBuilder().startObject().field("field", queryString).endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().get();
        TermVectorsRequestBuilder resp = client().prepareTermVectors("test", Integer.toString(1))
                .setPayloads(true).setOffsets(true).setPositions(true).setSelectedFields();
        TermVectorsResponse response = resp.execute().actionGet();
        assertThat("doc id 1 doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            String term = iterator.term().utf8ToString();
            PostingsEnum docsAndPositions = iterator.postings(null, PostingsEnum.ALL);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            List<BytesRef> curPayloads = payloads.get(term);
            assertThat(term, curPayloads, notNullValue());
            assertNotNull(docsAndPositions);
            for (int k = 0; k < docsAndPositions.freq(); k++) {
                docsAndPositions.nextPosition();
                if (docsAndPositions.getPayload()!=null){
                    String infoString = "\nterm: " + term + " has payload \n"+ docsAndPositions.getPayload().toString() +
                            "\n but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, docsAndPositions.getPayload(), equalTo(curPayloads.get(k)));
                } else {
                    String infoString = "\nterm: " + term + " has no payload but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, curPayloads.get(k).length, equalTo(0));
                }
            }
        }
        assertThat(iterator.next(), nullValue());
    }

    private String createString(String[] tokens, Map<String, List<BytesRef>> payloads, int encoding, char delimiter) {
        String resultString = "";
        Map<String, Integer> payloadCounter = new HashMap<>();
        for (String token : tokens) {
            if (!payloadCounter.containsKey(token)) {
                payloadCounter.putIfAbsent(token, 0);
            } else {
                payloadCounter.put(token, payloadCounter.get(token) + 1);
            }
            resultString = resultString + token;
            BytesRef payload = payloads.get(token).get(payloadCounter.get(token));
            if (payload.length > 0) {
                resultString = resultString + delimiter;
                switch (encoding) {
                    case 0: {
                        resultString = resultString + Float.toString(PayloadHelper.decodeFloat(payload.bytes, payload.offset));
                        break;
                    }
                    case 1: {
                        resultString = resultString + Integer.toString(PayloadHelper.decodeInt(payload.bytes, payload.offset));
                        break;
                    }
                    case 2: {
                        resultString = resultString + payload.utf8ToString();
                        break;
                    }
                    default: {
                        throw new ElasticsearchException("unsupported encoding type");
                    }
                }
            }
            resultString = resultString + " ";
        }
        return resultString;
    }

    private String[] crateRandomTokens() {
        String[] tokens = { "the", "quick", "brown", "fox" };
        int numTokensWithDuplicates = randomIntBetween(3, 15);
        String[] finalTokens = new String[numTokensWithDuplicates];
        for (int i = 0; i < numTokensWithDuplicates; i++) {
            finalTokens[i] = tokens[randomIntBetween(0, tokens.length - 1)];
        }
        return finalTokens;
    }

    private String createRandomDelimiter(String[] tokens) {
        String delimiter = "";
        boolean isTokenOrWhitespace = true;
        while(isTokenOrWhitespace) {
            isTokenOrWhitespace = false;
            delimiter = randomUnicodeOfLength(1);
            for(String token:tokens) {
                if(token.contains(delimiter)) {
                    isTokenOrWhitespace = true;
                }
            }
            if(Character.isWhitespace(delimiter.charAt(0))) {
                isTokenOrWhitespace = true;
            }
        }
        return delimiter;
    }

    private Map<String, List<BytesRef>> createPayloads(String[] tokens, int encoding) {
        Map<String, List<BytesRef>> payloads = new HashMap<>();
        for (String token : tokens) {
            payloads.computeIfAbsent(token, k -> new ArrayList<>());
            boolean createPayload = randomBoolean();
            if (createPayload) {
                switch (encoding) {
                    case 0: {
                        float theFloat = randomFloat();
                        payloads.get(token).add(new BytesRef(PayloadHelper.encodeFloat(theFloat)));
                        break;
                    }
                    case 1: {
                        payloads.get(token).add(new BytesRef(PayloadHelper.encodeInt(randomInt())));
                        break;
                    }
                    case 2: {
                        String payload = randomUnicodeOfLengthBetween(50, 100);
                        for (int c = 0; c < payload.length(); c++) {
                            if (Character.isWhitespace(payload.charAt(c))) {
                                payload = payload.replace(payload.charAt(c), 'w');
                            }
                        }
                        payloads.get(token).add(new BytesRef(payload));
                        break;
                    }
                    default: {
                        throw new ElasticsearchException("unsupported encoding type");
                    }
                }
            } else {
                payloads.get(token).add(new BytesRef());
            }
        }
        return payloads;
    }
}
