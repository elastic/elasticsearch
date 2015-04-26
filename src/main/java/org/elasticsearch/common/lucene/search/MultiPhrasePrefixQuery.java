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

package org.elasticsearch.common.lucene.search;

import com.carrotsearch.hppc.ObjectOpenHashSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class MultiPhrasePrefixQuery extends Query {

    private String field;
    private ArrayList<Term[]> termArrays = new ArrayList<>();
    private ArrayList<Integer> positions = new ArrayList<>();
    private int maxExpansions = Integer.MAX_VALUE;

    private int slop = 0;

    /**
     * Sets the phrase slop for this query.
     *
     * @see org.apache.lucene.search.PhraseQuery#setSlop(int)
     */
    public void setSlop(int s) {
        slop = s;
    }

    public void setMaxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
    }

    /**
     * Sets the phrase slop for this query.
     *
     * @see org.apache.lucene.search.PhraseQuery#getSlop()
     */
    public int getSlop() {
        return slop;
    }

    /**
     * Add a single term at the next position in the phrase.
     *
     * @see org.apache.lucene.search.PhraseQuery#add(Term)
     */
    public void add(Term term) {
        add(new Term[]{term});
    }

    /**
     * Add multiple terms at the next position in the phrase.  Any of the terms
     * may match.
     *
     * @see org.apache.lucene.search.PhraseQuery#add(Term)
     */
    public void add(Term[] terms) {
        int position = 0;
        if (positions.size() > 0)
            position = positions.get(positions.size() - 1).intValue() + 1;

        add(terms, position);
    }

    /**
     * Allows to specify the relative position of terms within the phrase.
     *
     * @param terms
     * @param position
     * @see org.apache.lucene.search.PhraseQuery#add(Term, int)
     */
    public void add(Term[] terms, int position) {
        if (termArrays.size() == 0)
            field = terms[0].field();

        for (int i = 0; i < terms.length; i++) {
            if (terms[i].field() != field) {
                throw new IllegalArgumentException(
                        "All phrase terms must be in the same field (" + field + "): "
                                + terms[i]);
            }
        }

        termArrays.add(terms);
        positions.add(Integer.valueOf(position));
    }

    /**
     * Returns a List of the terms in the multiphrase.
     * Do not modify the List or its contents.
     */
    public List<Term[]> getTermArrays() {
        return Collections.unmodifiableList(termArrays);
    }

    /**
     * Returns the relative positions of terms in this phrase.
     */
    public int[] getPositions() {
        int[] result = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++)
            result[i] = positions.get(i).intValue();
        return result;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (termArrays.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        MultiPhraseQuery query = new MultiPhraseQuery();
        query.setSlop(slop);
        int sizeMinus1 = termArrays.size() - 1;
        for (int i = 0; i < sizeMinus1; i++) {
            query.add(termArrays.get(i), positions.get(i));
        }
        Term[] suffixTerms = termArrays.get(sizeMinus1);
        int position = positions.get(sizeMinus1);
        ObjectOpenHashSet<Term> terms = new ObjectOpenHashSet<>();
        for (Term term : suffixTerms) {
            getPrefixTerms(terms, term, reader);
            if (terms.size() > maxExpansions) {
                break;
            }
        }
        if (terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery();
        }
        query.add(terms.toArray(Term.class), position);
        return query.rewrite(reader);
    }

    private void getPrefixTerms(ObjectOpenHashSet<Term> terms, final Term prefix, final IndexReader reader) throws IOException {
        // SlowCompositeReaderWrapper could be used... but this would merge all terms from each segment into one terms
        // instance, which is very expensive. Therefore I think it is better to iterate over each leaf individually.
        List<LeafReaderContext> leaves = reader.leaves();
        for (LeafReaderContext leaf : leaves) {
            Terms _terms = leaf.reader().terms(field);
            if (_terms == null) {
                continue;
            }

            TermsEnum termsEnum = _terms.iterator();
            TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(prefix.bytes());
            if (TermsEnum.SeekStatus.END == seekStatus) {
                continue;
            }

            for (BytesRef term = termsEnum.term(); term != null; term = termsEnum.next()) {
                if (!StringHelper.startsWith(term, prefix.bytes())) {
                    break;
                }

                terms.add(new Term(field, BytesRef.deepCopyOf(term)));
                if (terms.size() >= maxExpansions) {
                    return;
                }
            }
        }
    }

    @Override
    public final String toString(String f) {
        StringBuilder buffer = new StringBuilder();
        if (field == null || !field.equals(f)) {
            buffer.append(field);
            buffer.append(":");
        }

        buffer.append("\"");
        Iterator<Term[]> i = termArrays.iterator();
        while (i.hasNext()) {
            Term[] terms = i.next();
            if (terms.length > 1) {
                buffer.append("(");
                for (int j = 0; j < terms.length; j++) {
                    buffer.append(terms[j].text());
                    if (j < terms.length - 1) {
                        if (i.hasNext()) {
                            buffer.append(" ");
                        } else {
                            buffer.append("* ");
                        }
                    }
                }
                if (i.hasNext()) {
                    buffer.append(") ");
                } else {
                    buffer.append("*)");
                }
            } else {
                buffer.append(terms[0].text());
                if (i.hasNext()) {
                    buffer.append(" ");
                } else {
                    buffer.append("*");
                }
            }
        }
        buffer.append("\"");

        if (slop != 0) {
            buffer.append("~");
            buffer.append(slop);
        }

        buffer.append(ToStringUtils.boost(getBoost()));

        return buffer.toString();
    }

    /**
     * Returns true if <code>o</code> is equal to this.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MultiPhrasePrefixQuery)) return false;
        MultiPhrasePrefixQuery other = (MultiPhrasePrefixQuery) o;
        return this.getBoost() == other.getBoost()
                && this.slop == other.slop
                && termArraysEquals(this.termArrays, other.termArrays)
                && this.positions.equals(other.positions);
    }

    /**
     * Returns a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Float.floatToIntBits(getBoost())
                ^ slop
                ^ termArraysHashCode()
                ^ positions.hashCode()
                ^ 0x4AC65113;
    }

    // Breakout calculation of the termArrays hashcode
    private int termArraysHashCode() {
        int hashCode = 1;
        for (final Term[] termArray : termArrays) {
            hashCode = 31 * hashCode
                    + (termArray == null ? 0 : Arrays.hashCode(termArray));
        }
        return hashCode;
    }

    // Breakout calculation of the termArrays equals
    private boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
        if (termArrays1.size() != termArrays2.size()) {
            return false;
        }
        ListIterator<Term[]> iterator1 = termArrays1.listIterator();
        ListIterator<Term[]> iterator2 = termArrays2.listIterator();
        while (iterator1.hasNext()) {
            Term[] termArray1 = iterator1.next();
            Term[] termArray2 = iterator2.next();
            if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1,
                    termArray2))) {
                return false;
            }
        }
        return true;
    }

    public String getField() {
        return field;
    }
}
