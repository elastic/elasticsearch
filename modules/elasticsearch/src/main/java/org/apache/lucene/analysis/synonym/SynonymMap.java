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

package org.apache.lucene.analysis.synonym;

import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.util.Version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Mapping rules for use with {@link SynonymFilter}
 */
public class SynonymMap {
    /**
     * @lucene.internal
     */
    public CharArrayMap<SynonymMap> submap; // recursive: Map<String, SynonymMap>
    /**
     * @lucene.internal
     */
    public Token[] synonyms;
    int flags;

    static final int INCLUDE_ORIG = 0x01;
    static final int IGNORE_CASE = 0x02;

    public SynonymMap() {
    }

    public SynonymMap(boolean ignoreCase) {
        if (ignoreCase) flags |= IGNORE_CASE;
    }

    public boolean includeOrig() {
        return (flags & INCLUDE_ORIG) != 0;
    }

    public boolean ignoreCase() {
        return (flags & IGNORE_CASE) != 0;
    }

    /**
     * @param singleMatch   List<String>, the sequence of strings to match
     * @param replacement   List<Token> the list of tokens to use on a match
     * @param includeOrig   sets a flag on this mapping signaling the generation of matched tokens in addition to the replacement tokens
     * @param mergeExisting merge the replacement tokens with any other mappings that exist
     */
    public void add(List<String> singleMatch, List<Token> replacement, boolean includeOrig, boolean mergeExisting) {
        SynonymMap currMap = this;
        for (String str : singleMatch) {
            if (currMap.submap == null) {
                // for now hardcode at 4.0, as its what the old code did.
                // would be nice to fix, but shouldn't store a version in each submap!!!
                currMap.submap = new CharArrayMap<SynonymMap>(Version.LUCENE_31, 1, ignoreCase());
            }

            SynonymMap map = currMap.submap.get(str);
            if (map == null) {
                map = new SynonymMap();
                map.flags |= flags & IGNORE_CASE;
                currMap.submap.put(str, map);
            }

            currMap = map;
        }

        if (currMap.synonyms != null && !mergeExisting) {
            throw new RuntimeException("SynonymFilter: there is already a mapping for " + singleMatch);
        }
        List<Token> superset = currMap.synonyms == null ? replacement :
                mergeTokens(Arrays.asList(currMap.synonyms), replacement);
        currMap.synonyms = superset.toArray(new Token[superset.size()]);
        if (includeOrig) currMap.flags |= INCLUDE_ORIG;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("<");
        if (synonyms != null) {
            sb.append("[");
            for (int i = 0; i < synonyms.length; i++) {
                if (i != 0) sb.append(',');
                sb.append(synonyms[i]);
            }
            if ((flags & INCLUDE_ORIG) != 0) {
                sb.append(",ORIG");
            }
            sb.append("],");
        }
        sb.append(submap);
        sb.append(">");
        return sb.toString();
    }


    /**
     * Produces a List<Token> from a List<String>
     */
    public static List<Token> makeTokens(List<String> strings) {
        List<Token> ret = new ArrayList<Token>(strings.size());
        for (String str : strings) {
            //Token newTok = new Token(str,0,0,"SYNONYM");
            Token newTok = new Token(str, 0, 0, "SYNONYM");
            ret.add(newTok);
        }
        return ret;
    }


    /**
     * Merge two lists of tokens, producing a single list with manipulated positionIncrements so that
     * the tokens end up at the same position.
     *
     * Example:  [a b] merged with [c d] produces [a/b c/d]  ('/' denotes tokens in the same position)
     * Example:  [a,5 b,2] merged with [c d,4 e,4] produces [c a,5/d b,2 e,2]  (a,n means a has posInc=n)
     */
    public static List<Token> mergeTokens(List<Token> lst1, List<Token> lst2) {
        ArrayList<Token> result = new ArrayList<Token>();
        if (lst1 == null || lst2 == null) {
            if (lst2 != null) result.addAll(lst2);
            if (lst1 != null) result.addAll(lst1);
            return result;
        }

        int pos = 0;
        Iterator<Token> iter1 = lst1.iterator();
        Iterator<Token> iter2 = lst2.iterator();
        Token tok1 = iter1.hasNext() ? iter1.next() : null;
        Token tok2 = iter2.hasNext() ? iter2.next() : null;
        int pos1 = tok1 != null ? tok1.getPositionIncrement() : 0;
        int pos2 = tok2 != null ? tok2.getPositionIncrement() : 0;
        while (tok1 != null || tok2 != null) {
            while (tok1 != null && (pos1 <= pos2 || tok2 == null)) {
                Token tok = new Token(tok1.startOffset(), tok1.endOffset(), tok1.type());
                tok.copyBuffer(tok1.buffer(), 0, tok1.length());
                tok.setPositionIncrement(pos1 - pos);
                result.add(tok);
                pos = pos1;
                tok1 = iter1.hasNext() ? iter1.next() : null;
                pos1 += tok1 != null ? tok1.getPositionIncrement() : 0;
            }
            while (tok2 != null && (pos2 <= pos1 || tok1 == null)) {
                Token tok = new Token(tok2.startOffset(), tok2.endOffset(), tok2.type());
                tok.copyBuffer(tok2.buffer(), 0, tok2.length());
                tok.setPositionIncrement(pos2 - pos);
                result.add(tok);
                pos = pos2;
                tok2 = iter2.hasNext() ? iter2.next() : null;
                pos2 += tok2 != null ? tok2.getPositionIncrement() : 0;
            }
        }
        return result;
    }

}
