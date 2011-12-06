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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

/**
 * A token filter that truncates tokens.
 */
public class TruncateTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);

    private final int size;

    public TruncateTokenFilter(TokenStream in, int size) {
        super(in);
        this.size = size;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            final int length = termAttribute.length();
            if (length > size) {
                termAttribute.setLength(size);
            }
            return true;
        } else {
            return false;
        }
    }
}


