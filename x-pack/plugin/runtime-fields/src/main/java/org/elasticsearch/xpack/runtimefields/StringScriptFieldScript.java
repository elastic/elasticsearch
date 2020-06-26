/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public abstract class StringScriptFieldScript extends AbstractScriptFieldScript {
    static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("string_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "string_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SourceLookup source, DocLookup fieldData);
    }

    public interface LeafFactory {
        StringScriptFieldScript newInstance(LeafReaderContext ctx, Consumer<String> sync) throws IOException;

        default Query termQuery(String fieldName, String value) {
            return new ScriptTermQuery(this, fieldName, value);
        }

        default Query prefixQuery(String fieldName, String value) {
            return new ScriptPrefixQuery(this, fieldName, value);
        }
    }

    private final Consumer<String> sync;

    public StringScriptFieldScript(
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData,
        LeafReaderContext ctx,
        Consumer<String> sync
    ) {
        super(params, source, fieldData, ctx);
        this.sync = sync;
    }

    public static class Value {
        private final StringScriptFieldScript script;

        public Value(StringScriptFieldScript script) {
            this.script = script;
        }

        public void value(String v) {
            script.sync.accept(v);
        }
    }

    private static class ScriptTermQuery extends Query {
        private final LeafFactory leafFactory;
        private final String fieldName;
        private final String term;

        private ScriptTermQuery(LeafFactory leafFactory, String fieldName, String term) {
            this.leafFactory = leafFactory;
            this.fieldName = fieldName;
            this.term = term;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false; // scripts aren't really cacheable at this point
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                    TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                        private boolean hasMatches;
                        private final StringScriptFieldScript script = leafFactory.newInstance(
                            context,
                            s -> { hasMatches = hasMatches || term.equals(s); }
                        );

                        @Override
                        public boolean matches() throws IOException {
                            hasMatches = false;
                            script.setDocument(approximation.docID());
                            script.execute();
                            return hasMatches;
                        }

                        @Override
                        public float matchCost() {
                            // TODO we have no idea what this should be and no real way to get one
                            return 1000f;
                        }
                    };
                    return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
                }

                @Override
                public void extractTerms(Set<Term> terms) {
                    terms.add(new Term(fieldName, term));
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.consumeTerms(this, new Term(fieldName, term));
        }

        @Override
        public String toString(String field) {
            if (fieldName.contentEquals(field)) {
                return term;
            }
            return fieldName + ":" + term;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, term);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ScriptTermQuery other = (ScriptTermQuery) obj;
            return fieldName.equals(other.fieldName) && term.equals(other.term);
        }
    }

    private static class ScriptPrefixQuery extends Query {
        private final LeafFactory leafFactory;
        private final String fieldName;
        private final String prefix;

        private ScriptPrefixQuery(LeafFactory leafFactory, String fieldName, String prefix) {
            this.leafFactory = leafFactory;
            this.fieldName = fieldName;
            this.prefix = prefix;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false; // scripts aren't really cacheable at this point
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                    TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                        private boolean hasMatches;
                        private final StringScriptFieldScript script = leafFactory.newInstance(
                            context,
                            s -> { hasMatches = hasMatches || s.startsWith(prefix); }
                        );

                        @Override
                        public boolean matches() throws IOException {
                            hasMatches = false;
                            script.setDocument(approximation.docID());
                            script.execute();
                            return hasMatches;
                        }

                        @Override
                        public float matchCost() {
                            // TODO we have no idea what this should be and no real way to get one
                            return 1000f;
                        }
                    };
                    return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
                }

                @Override
                public void extractTerms(Set<Term> terms) {
                    // TODO doing this is sort of difficult and maybe not needed.
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.consumeTermsMatching(
                this,
                fieldName,
                () -> new ByteRunAutomaton(PrefixQuery.toAutomaton(new BytesRef(prefix)), true, Integer.MAX_VALUE)
            );
        }

        @Override
        public String toString(String field) {
            if (fieldName.contentEquals(field)) {
                return prefix + "*";
            }
            return fieldName + ":" + prefix + "*";
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, prefix);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ScriptPrefixQuery other = (ScriptPrefixQuery) obj;
            return fieldName.equals(other.fieldName) && prefix.equals(other.prefix);
        }
    }
}
