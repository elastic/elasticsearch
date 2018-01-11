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

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DeDuplicatingTokenFilterTests extends ESTestCase {
    public void testSimple() throws IOException {
        DuplicateByteSequenceSpotter bytesDeDuper = new DuplicateByteSequenceSpotter();
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new DeDuplicatingTokenFilter(t, bytesDeDuper));
            }
        };

        String input = "a b c 1 2 3 4 5 6 7 a b c d 1 2 3 4 5 6 7 e f 1 2 3 4 5 6 7";
        String expectedOutput = "a b c 1 2 3 4 5 6 7 a b c d e f";
        TokenStream test = analyzer.tokenStream("test", input);
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);

        test.reset();

        StringBuilder sb = new StringBuilder();
        while (test.incrementToken()) {
            sb.append(termAttribute.toString());
            sb.append(" ");
        }
        String output = sb.toString().trim();
        assertThat(output, equalTo(expectedOutput));

    }
    
    public void testHitCountLimits() throws IOException {
        DuplicateByteSequenceSpotter bytesDeDuper = new DuplicateByteSequenceSpotter();
        long peakMemoryUsed = 0;
        for (int i = 0; i < DuplicateByteSequenceSpotter.MAX_HIT_COUNT * 2; i++) {
            Analyzer analyzer = new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                    return new TokenStreamComponents(t, new DeDuplicatingTokenFilter(t, bytesDeDuper, true));
                }
            };
            try {
                String input = "1 2 3 4 5 6";
                bytesDeDuper.startNewSequence();

                TokenStream test = analyzer.tokenStream("test", input);
                DuplicateSequenceAttribute dsa = test.addAttribute(DuplicateSequenceAttribute.class);

                test.reset();

                while (test.incrementToken()) {
                    assertEquals(Math.min(DuplicateByteSequenceSpotter.MAX_HIT_COUNT, i), dsa.getNumPriorUsesInASequence());
                }

                if (i == 0) {
                    peakMemoryUsed = bytesDeDuper.getEstimatedSizeInBytes();
                } else {
                    // Given we are feeding the same content repeatedly the
                    // actual memory
                    // used by bytesDeDuper should not grow
                    assertEquals(peakMemoryUsed, bytesDeDuper.getEstimatedSizeInBytes());
                }

            } finally {
                analyzer.close();
            }
        }
    }

    public void testTaggedFrequencies() throws IOException {
        DuplicateByteSequenceSpotter bytesDeDuper = new DuplicateByteSequenceSpotter();
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(t, new DeDuplicatingTokenFilter(t, bytesDeDuper, true));
            }
        };
        try {
            String input = "a b c 1 2 3 4 5 6 7 a b c d 1 2 3 4 5 6 7 e f 1 2 3 4 5 6 7";
            short[] expectedFrequencies = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 2, 2, 2, 2, 2, 2, 2 };
            TokenStream test = analyzer.tokenStream("test", input);
            DuplicateSequenceAttribute seqAtt = test.addAttribute(DuplicateSequenceAttribute.class);
    
            test.reset();
    
            for (int i = 0; i < expectedFrequencies.length; i++) {
                assertThat(test.incrementToken(), equalTo(true));
                assertThat(seqAtt.getNumPriorUsesInASequence(), equalTo(expectedFrequencies[i]));
            }
            assertThat(test.incrementToken(), equalTo(false));
        } finally {
            analyzer.close();
        }

    }
}