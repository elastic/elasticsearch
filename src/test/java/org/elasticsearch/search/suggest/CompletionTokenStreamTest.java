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
package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.SynonymMap.Builder;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.search.suggest.completion.CompletionTokenStream;
import org.elasticsearch.search.suggest.completion.CompletionTokenStream.ByteTermAttribute;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CompletionTokenStreamTest extends ElasticsearchTokenStreamTestCase {

    final XAnalyzingSuggester suggester = new XAnalyzingSuggester(new SimpleAnalyzer());

    @Test
    public void testSuggestTokenFilter() throws Exception {
        Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        tokenStream.setReader(new StringReader("mykeyword"));
        BytesRef payload = new BytesRef("Surface keyword|friggin payload|10");
        TokenStream suggestTokenStream = new ByteTermAttrToCharTermAttrFilter(new CompletionTokenStream(tokenStream, payload, new CompletionTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                return suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
            }
        }));
        assertTokenStreamContents(suggestTokenStream, new String[] {"mykeyword"}, null, null, new String[] {"Surface keyword|friggin payload|10"}, new int[] { 1 }, null, null);
    }

    @Test
    public void testSuggestTokenFilterWithSynonym() throws Exception {
        Builder builder = new SynonymMap.Builder(true);
        builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);

        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        tokenizer.setReader(new StringReader("mykeyword"));
        SynonymFilter filter = new SynonymFilter(tokenizer, builder.build(), true);

        BytesRef payload = new BytesRef("Surface keyword|friggin payload|10");
        TokenStream suggestTokenStream = new ByteTermAttrToCharTermAttrFilter(new CompletionTokenStream(filter, payload, new CompletionTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                return suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
            }
        }));
        assertTokenStreamContents(suggestTokenStream, new String[] {"mysynonym", "mykeyword"}, null, null, new String[] {"Surface keyword|friggin payload|10", "Surface keyword|friggin payload|10"}, new int[] { 2, 0 }, null, null);
    }

    @Test
    public void testValidNumberOfExpansions() throws IOException {
        Builder builder = new SynonymMap.Builder(true);
        for (int i = 0; i < 256; i++) {
            builder.add(new CharsRef("" + (i+1)), new CharsRef("" + (1000 + (i+1))), true);
        }
        StringBuilder valueBuilder = new StringBuilder();
        for (int i = 0 ; i < 8 ; i++) {
            valueBuilder.append(i+1);
            valueBuilder.append(" ");
        }
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        tokenizer.setReader(new StringReader(valueBuilder.toString()));
        SynonymFilter filter = new SynonymFilter(tokenizer, builder.build(), true);
       
        TokenStream suggestTokenStream = new CompletionTokenStream(filter, new BytesRef("Surface keyword|friggin payload|10"), new CompletionTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                Set<IntsRef> finiteStrings = suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
                return finiteStrings;
            }
        });
        
        suggestTokenStream.reset();
        ByteTermAttribute attr = suggestTokenStream.addAttribute(ByteTermAttribute.class);
        PositionIncrementAttribute posAttr = suggestTokenStream.addAttribute(PositionIncrementAttribute.class);
        int maxPos = 0;
        int count = 0;
        while(suggestTokenStream.incrementToken()) {
            count++;
            assertNotNull(attr.getBytesRef());
            assertTrue(attr.getBytesRef().length > 0);
            maxPos += posAttr.getPositionIncrement();
        }
        suggestTokenStream.close();
        assertEquals(count, 256);
        assertEquals(count, maxPos);

    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInValidNumberOfExpansions() throws IOException {
        Builder builder = new SynonymMap.Builder(true);
        for (int i = 0; i < 256; i++) {
            builder.add(new CharsRef("" + (i+1)), new CharsRef("" + (1000 + (i+1))), true);
        }
        StringBuilder valueBuilder = new StringBuilder();
        for (int i = 0 ; i < 9 ; i++) { // 9 -> expands to 512
            valueBuilder.append(i+1);
            valueBuilder.append(" ");
        }
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        tokenizer.setReader(new StringReader(valueBuilder.toString()));
        SynonymFilter filter = new SynonymFilter(tokenizer, builder.build(), true);
       
        TokenStream suggestTokenStream = new CompletionTokenStream(filter, new BytesRef("Surface keyword|friggin payload|10"), new CompletionTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                Set<IntsRef> finiteStrings = suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
                return finiteStrings;
            }
        });
        
        suggestTokenStream.reset();
        suggestTokenStream.incrementToken();
        suggestTokenStream.close();

    }

    @Test
    public void testSuggestTokenFilterProperlyDelegateInputStream() throws Exception {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        tokenizer.setReader(new StringReader("mykeyword"));
        BytesRef payload = new BytesRef("Surface keyword|friggin payload|10");
        TokenStream suggestTokenStream = new ByteTermAttrToCharTermAttrFilter(new CompletionTokenStream(tokenizer, payload, new CompletionTokenStream.ToFiniteStrings() {
            @Override
            public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
                return suggester.toFiniteStrings(suggester.getTokenStreamToAutomaton(), stream);
            }
        }));
        TermToBytesRefAttribute termAtt = suggestTokenStream.getAttribute(TermToBytesRefAttribute.class);
        BytesRef ref = termAtt.getBytesRef();
        assertNotNull(ref);
        suggestTokenStream.reset();

        while (suggestTokenStream.incrementToken()) {
            termAtt.fillBytesRef();
            assertThat(ref.utf8ToString(), equalTo("mykeyword"));
        }
        suggestTokenStream.end();
        suggestTokenStream.close();
    }


    public final static class ByteTermAttrToCharTermAttrFilter extends TokenFilter {
        private ByteTermAttribute byteAttr = addAttribute(ByteTermAttribute.class);
        private PayloadAttribute payload = addAttribute(PayloadAttribute.class);
        private TypeAttribute type = addAttribute(TypeAttribute.class);
        private CharTermAttribute charTermAttribute = addAttribute(CharTermAttribute.class);
        protected ByteTermAttrToCharTermAttrFilter(TokenStream input) {
            super(input);
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken()) {
                BytesRef bytesRef = byteAttr.getBytesRef();
                // we move them over so we can assert them more easily in the tests
                type.setType(payload.getPayload().utf8ToString());
                return true;
            }
            return false;
        }

    }
}
