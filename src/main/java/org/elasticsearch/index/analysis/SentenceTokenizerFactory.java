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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;
import java.text.BreakIterator;
import java.util.Locale;

/**
 * Based on http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/analysis/common/src/test/org/apache/lucene/analysis/util/TestSegmentingTokenizerBase.java?revision=1664404&view=markup
 */
public class SentenceTokenizerFactory extends AbstractTokenizerFactory {

    @Inject
    public SentenceTokenizerFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
    }

    @Override
    public Tokenizer create(Reader reader) {
        return new WholeSentenceTokenizer(reader);
    }

    static class WholeSentenceTokenizer extends SegmentingTokenizerBase {
        int sentenceStart, sentenceEnd;
        boolean hasSentence;

        private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

        public WholeSentenceTokenizer(Reader reader) {
            super(reader, BreakIterator.getSentenceInstance(Locale.ROOT));
        }

        @Override
        protected void setNextSentence(int sentenceStart, int sentenceEnd) {
            this.sentenceStart = sentenceStart;
            this.sentenceEnd = sentenceEnd;
            hasSentence = true;
        }

        @Override
        protected boolean incrementWord() {
            if (hasSentence) {
                hasSentence = false;
                clearAttributes();
                termAtt.copyBuffer(buffer, sentenceStart, sentenceEnd-sentenceStart);
                offsetAtt.setOffset(correctOffset(offset+sentenceStart), correctOffset(offset+sentenceEnd));
                return true;
            } else {
                return false;
            }
        }
    }
}
