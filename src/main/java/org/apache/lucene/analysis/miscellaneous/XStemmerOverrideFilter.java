/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;

/**
 * Provides the ability to override any {@link KeywordAttribute} aware stemmer
 * with custom dictionary-based stemming.
 */
// LUCENE UPGRADE - this is a copy of the StemmerOverrideFilter from Lucene - update with 4.3
public final class XStemmerOverrideFilter extends TokenFilter {
    private final FST<BytesRef> fst;
    
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
    private final BytesReader fstReader;
    private final Arc<BytesRef> scratchArc = new FST.Arc<BytesRef>();
  ;
    private final CharsRef spare = new CharsRef();
    private final boolean ignoreCase;
    
    /**
     * Create a new StemmerOverrideFilter, performing dictionary-based stemming
     * with the provided <code>dictionary</code>.
     * <p>
     * Any dictionary-stemmed terms will be marked with {@link KeywordAttribute}
     * so that they will not be stemmed with stemmers down the chain.
     * </p>
     */
    public XStemmerOverrideFilter(TokenStream input, StemmerOverrideMap stemmerOverrideMap, boolean ignoreCase) {
      super(input);
      this.fst = stemmerOverrideMap.fst;
      fstReader = fst.getBytesReader();
      this.ignoreCase = ignoreCase;
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (!keywordAtt.isKeyword()) { // don't muck with already-keyworded terms
          final BytesRef stem = getStem(termAtt.buffer(), termAtt.length());
          if (stem != null) {
            final char[] buffer = spare.chars = termAtt.buffer();
            UnicodeUtil.UTF8toUTF16(stem.bytes, stem.offset, stem.length, spare);
            if (spare.chars != buffer) {
              termAtt.copyBuffer(spare.chars, spare.offset, spare.length);
            }
            termAtt.setLength(spare.length);
            keywordAtt.setKeyword(true);
          }
        }
        return true;
      } else {
        return false;
      }
    }
    
    private BytesRef getStem(char[] buffer, int bufferLen) throws IOException {
      BytesRef pendingOutput = fst.outputs.getNoOutput();
      BytesRef matchOutput = null;
      int bufUpto = 0;
      fst.getFirstArc(scratchArc);
      while (bufUpto < bufferLen) {
        final int codePoint = Character.codePointAt(buffer, bufUpto, bufferLen);
        if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) == null) {
          return null;
        }
        pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
        bufUpto += Character.charCount(codePoint);
      }
      if (scratchArc.isFinal()) {
        matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
      }
      return matchOutput;
    }
    
    
    public static class StemmerOverrideMap {
      final FST<BytesRef> fst;
      
      StemmerOverrideMap(FST<BytesRef> fst) {
        this.fst = fst;
      }
      
    }
    /**
     * This builder builds an {@link FST} for the {@link StemmerOverrideFilter}
     */
    public static class Builder {
      private final BytesRefHash hash = new BytesRefHash();
      private final BytesRef spare = new BytesRef();
      private final ArrayList<CharSequence> outputValues = new ArrayList<CharSequence>();
      /**
       * Adds an input string and it's stemmer overwrite output to this builder.
       * 
       * @param input the input char sequence 
       * @param output the stemmer override output char sequence
       * @return <code>false</code> iff the input has already been added to this builder otherwise <code>true</code>.
       */
      public boolean add(CharSequence input, CharSequence output) {
        UnicodeUtil.UTF16toUTF8(input, 0, input.length(), spare);
        int id = hash.add(spare);
        if (id >= 0) {
          outputValues.add(output);
          return true;
        }
        return false;
      }
      
      /**
       * Returns an {@link StemmerOverrideMap} to be used with the {@link StemmerOverrideFilter}
       * @return an {@link StemmerOverrideMap} to be used with the {@link StemmerOverrideFilter}
       * @throws IOException if an {@link IOException} occurs;
       */
      public StemmerOverrideMap build() throws IOException {
        ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        org.apache.lucene.util.fst.Builder<BytesRef> builder = new org.apache.lucene.util.fst.Builder<BytesRef>(
            FST.INPUT_TYPE.BYTE4, outputs);
        final int[] sort = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
        IntsRef intsSpare = new IntsRef();
        final int size = hash.size();
        for (int i = 0; i < size; i++) {
          int id = sort[i];
          BytesRef bytesRef = hash.get(id, spare);
          UnicodeUtil.UTF8toUTF32(bytesRef, intsSpare);
          builder.add(intsSpare, new BytesRef(outputValues.get(id)));
        }
        return new StemmerOverrideMap(builder.finish());
      }
      
    }
}
