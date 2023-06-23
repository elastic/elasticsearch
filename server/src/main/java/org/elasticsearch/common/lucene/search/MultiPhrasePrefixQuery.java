/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;

public class MultiPhrasePrefixQuery extends Query {

    private final String field;
    private final ArrayList<Term[]> termArrays = new ArrayList<>();
    private final ArrayList<Integer> positions = new ArrayList<>();
    private int maxExpansions = Integer.MAX_VALUE;

    private int slop = 0;

    public MultiPhrasePrefixQuery(String field) {
        this.field = Objects.requireNonNull(field);
    }

    /**
     * Sets the phrase slop for this query.
     *
     * @see org.apache.lucene.search.PhraseQuery.Builder#setSlop(int)
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
     * @see org.apache.lucene.search.PhraseQuery.Builder#getSlop()
     */
    public int getSlop() {
        return slop;
    }

    /**
     * Add a single term at the next position in the phrase.
     *
     * @see org.apache.lucene.search.PhraseQuery.Builder#add(Term)
     */
    public void add(Term term) {
        add(new Term[] { term });
    }

    /**
     * Add multiple terms at the next position in the phrase.  Any of the terms
     * may match.
     *
     * @see org.apache.lucene.search.PhraseQuery.Builder#add(Term)
     */
    public void add(Term[] terms) {
        int position = 0;
        if (positions.size() > 0) position = positions.get(positions.size() - 1) + 1;

        add(terms, position);
    }

    /**
     * Allows to specify the relative position of terms within the phrase.
     *
     * @param terms the terms
     * @param position the position of the terms provided as argument
     * @see org.apache.lucene.search.PhraseQuery.Builder#add(Term, int)
     */
    public void add(Term[] terms, int position) {
        for (int i = 0; i < terms.length; i++) {
            if (Objects.equals(terms[i].field(), field) == false) {
                throw new IllegalArgumentException("All phrase terms must be in the same field (" + field + "): " + terms[i]);
            }
        }

        termArrays.add(terms);
        positions.add(position);
    }

    /**
     * Returns the terms for each position in this phrase
     */
    public Term[][] getTerms() {
        Term[][] terms = new Term[termArrays.size()][];
        for (int i = 0; i < termArrays.size(); i++) {
            terms[i] = new Term[termArrays.get(i).length];
            System.arraycopy(termArrays.get(i), 0, terms[i], 0, termArrays.get(i).length);
        }
        return terms;
    }

    /**
     * Returns the relative positions of terms in this phrase.
     */
    public int[] getPositions() {
        int[] result = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++)
            result[i] = positions.get(i);
        return result;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = super.rewrite(searcher);
        if (rewritten != this) {
            return rewritten;
        }
        if (termArrays.isEmpty()) {
            return new MatchNoDocsQuery();
        }
        MultiPhraseQuery.Builder query = new MultiPhraseQuery.Builder();
        query.setSlop(slop);
        int sizeMinus1 = termArrays.size() - 1;
        for (int i = 0; i < sizeMinus1; i++) {
            query.add(termArrays.get(i), positions.get(i));
        }
        Term[] suffixTerms = termArrays.get(sizeMinus1);
        int position = positions.get(sizeMinus1);
        Set<Term> terms = new HashSet<>();
        for (Term term : suffixTerms) {
            getPrefixTerms(terms, term, searcher.getIndexReader());
            if (terms.size() > maxExpansions) {
                break;
            }
        }
        if (terms.isEmpty()) {
            return Queries.newMatchNoDocsQuery("No terms supplied for " + MultiPhrasePrefixQuery.class.getName());
        }
        query.add(terms.toArray(new Term[0]), position);
        return query.build();
    }

    private void getPrefixTerms(Set<Term> terms, final Term prefix, final IndexReader reader) throws IOException {
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
                if (StringHelper.startsWith(term, prefix.bytes()) == false) {
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
        if (field.equals(f) == false) {
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

        return buffer.toString();
    }

    /**
     * Returns true if <code>o</code> is equal to this.
     */
    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) {
            return false;
        }
        MultiPhrasePrefixQuery other = (MultiPhrasePrefixQuery) o;
        return this.slop == other.slop && termArraysEquals(this.termArrays, other.termArrays) && this.positions.equals(other.positions);
    }

    /**
     * Returns a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return classHash() ^ slop ^ termArraysHashCode() ^ positions.hashCode();
    }

    // Breakout calculation of the termArrays hashcode
    private int termArraysHashCode() {
        int hashCode = 1;
        for (final Term[] termArray : termArrays) {
            hashCode = 31 * hashCode + (termArray == null ? 0 : Arrays.hashCode(termArray));
        }
        return hashCode;
    }

    // Breakout calculation of the termArrays equals
    private static boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
        if (termArrays1.size() != termArrays2.size()) {
            return false;
        }
        ListIterator<Term[]> iterator1 = termArrays1.listIterator();
        ListIterator<Term[]> iterator2 = termArrays2.listIterator();
        while (iterator1.hasNext()) {
            Term[] termArray1 = iterator1.next();
            Term[] termArray2 = iterator2.next();
            if ((termArray1 == null ? termArray2 == null : Arrays.equals(termArray1, termArray2)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
            for (int i = 0; i < termArrays.size() - 1; i++) {
                if (termArrays.get(i).length == 1) {
                    visitor.consumeTerms(this, termArrays.get(i)[0]);
                } else {
                    QueryVisitor shouldVisitor = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
                    shouldVisitor.consumeTerms(this, termArrays.get(i));
                }
            }
            /* We don't report automata here because this breaks the unified highlighter,
               which extracts automata separately from phrases. MPPQ gets rewritten to a
               SpanMTQQuery by the PhraseHelper in any case, so highlighting is taken
               care of there instead.  If we extract automata here then the trailing prefix
               word will be highlighted wherever it appears in the document, instead of only
               as part of a phrase. This can be re-instated once we switch to using Matches
               to highlight.
            for (Term prefixTerm : termArrays.get(termArrays.size() - 1)) {
                visitor.consumeTermsMatching(this, field, () -> {
                    CompiledAutomaton ca = new CompiledAutomaton(PrefixQuery.toAutomaton(prefixTerm.bytes()));
                    return ca.runAutomaton;
                });
            }
            */
        }
    }
}
