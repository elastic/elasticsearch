/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public final class StringRuntimeValues extends AbstractRuntimeValues<StringRuntimeValues.SharedValues> {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, Consumer<String> sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public StringRuntimeValues(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValues() {
        return unstarted().docValues();
    }

    public Query termQuery(String fieldName, String value) {
        return unstarted().new TermQuery(fieldName, value);
    }

    public Query prefixQuery(String fieldName, String value) {
        return unstarted().new PrefixQuery(fieldName, value);
    }

    @Override
    protected SharedValues newSharedValues() {
        return new SharedValues();
    }

    protected class SharedValues extends AbstractRuntimeValues<SharedValues>.SharedValues {
        private String[] values = new String[1];

        @Override
        protected IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException {
            return newLeafLoader.leafLoader(ctx, this::add);
        }

        private void add(String value) {
            if (value == null) {
                return;
            }
            int newCount = count + 1;
            if (values.length < newCount) {
                values = Arrays.copyOf(values, ArrayUtil.oversize(newCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
            }
            values[count] = value;
            count = newCount;
        }

        @Override
        protected void sort() {
            Arrays.sort(values, 0, count);
        }

        private CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValues() {
            alwaysSortResults();
            return DocValues::new;
        }

        private class DocValues extends SortedBinaryDocValues {
            private final BytesRefBuilder ref = new BytesRefBuilder();
            private final IntConsumer leafCursor;
            private int next;

            DocValues(LeafReaderContext ctx) throws IOException {
                leafCursor = leafCursor(ctx);
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                leafCursor.accept(docId);
                next = 0;
                return count > 0;
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                ref.copyChars(values[next++]);
                return ref.get();
            }
        }

        private class TermQuery extends AbstractRuntimeQuery {
            private final String term;

            private TermQuery(String fieldName, String term) {
                super(fieldName);
                this.term = term;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (term.equals(values[i])) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, term));
            }

            @Override
            protected String bareToString() {
                return term;
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), term);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                TermQuery other = (TermQuery) obj;
                return term.equals(other.term);
            }
        }

        private class PrefixQuery extends AbstractRuntimeQuery {
            private final String prefix;

            private PrefixQuery(String fieldName, String prefix) {
                super(fieldName);
                this.prefix = prefix;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (values[i].startsWith(prefix)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String bareToString() {
                return prefix + "*";
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTermsMatching(
                    this,
                    fieldName,
                    () -> new ByteRunAutomaton(
                        org.apache.lucene.search.PrefixQuery.toAutomaton(new BytesRef(prefix)),
                        true,
                        Integer.MAX_VALUE
                    )
                );
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), prefix);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                PrefixQuery other = (PrefixQuery) obj;
                return prefix.equals(other.prefix);
            }
        }
    }
}
