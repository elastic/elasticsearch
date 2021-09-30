/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import com.carrotsearch.hppc.IntArrayList;
import com.ibm.icu.text.Normalizer;
import com.ibm.icu.text.Normalizer2;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.PrimitiveIterator;

public class StripAccentTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final Normalizer2 normalizer;
    private final StringBuilder buffer = new StringBuilder();

    public StripAccentTokenFilter(TokenStream input) {
        super(input);
        this.normalizer = Normalizer2.getNFDInstance();
    }

    @Override
    public final boolean incrementToken() throws IOException {
        // TODO seems like this + lowercase + tokenize cjk could all be the same thing....
        if (input.incrementToken()) {
            if (normalizer.quickCheck(termAtt) != Normalizer.YES) {
                buffer.setLength(0);
                normalizer.normalize(termAtt, buffer);
                IntArrayList badIndices = new IntArrayList();
                IntArrayList charCount = new IntArrayList();
                int index = 0;
                for (PrimitiveIterator.OfInt it = buffer.codePoints().iterator(); it.hasNext();) {
                    int cp = it.next();
                    if (Character.getType(cp) == Character.NON_SPACING_MARK) {
                        badIndices.add(index);
                        charCount.add(Character.charCount(cp));
                    }
                    index++;
                }
                for (int i = 0; i < badIndices.size(); i++) {
                    int badIndex = badIndices.get(i);
                    int count = charCount.get(i);
                    for (int j = 0; j < count && badIndex < buffer.length(); j++) {
                        buffer.deleteCharAt(badIndex);
                    }
                }
                termAtt.setEmpty().append(buffer);
            }
            return true;
        } else {
            return false;
        }
    }
}
