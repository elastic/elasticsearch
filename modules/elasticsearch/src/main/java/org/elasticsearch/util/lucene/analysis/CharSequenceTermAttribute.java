/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.lucene.analysis;

import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: Once 3.1 is out, no need for this class anymore, use CharTermAttribute
public class CharSequenceTermAttribute implements CharSequence {

    private final TermAttribute termAtt;

    public CharSequenceTermAttribute(TermAttribute termAtt) {
        this.termAtt = termAtt;
    }

    @Override public int length() {
        return termAtt.termLength();
    }

    @Override public char charAt(int index) {
        if (index >= length())
            throw new IndexOutOfBoundsException();
        return termAtt.termBuffer()[index];
    }

    @Override public CharSequence subSequence(int start, int end) {
        if (start > length() || end > length())
            throw new IndexOutOfBoundsException();
        return new String(termAtt.termBuffer(), start, end - start);
    }
}
