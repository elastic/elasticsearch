/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.RegExp.Kind;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A {@link FieldMapper} for indexing fields with ngrams for efficient wildcard matching
 */
public class WildcardFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "wildcard";
    public static short MAX_CLAUSES_IN_APPROXIMATION_QUERY = 10;
    public static final int NGRAM_SIZE = 3;
    static final NamedAnalyzer WILDCARD_ANALYZER = new NamedAnalyzer("_wildcard", AnalyzerScope.GLOBAL, new Analyzer() {
        @Override
        public TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new NGramTokenizer(NGRAM_SIZE, NGRAM_SIZE);
            return new TokenStreamComponents(tokenizer);
        }
    });

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new WildcardFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexAnalyzer(WILDCARD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStoreTermVectorOffsets(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
        public static final String NULL_VALUE = null;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        protected String nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues == false) {
                throw new MapperParsingException("The field [" + name + "] cannot have doc values = false");
            }
            return this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions != IndexOptions.DOCS) {
                throw new MapperParsingException("The field [" + name + "] cannot have indexOptions = " + indexOptions);
            }
            return this;
        }

        @Override
        public Builder store(boolean store) {
            if (store) {
                throw new MapperParsingException("The field [" + name + "] cannot have store = true");
            }
            return this;
        }

        @Override
        public Builder similarity(SimilarityProvider similarity) {
            throw new MapperParsingException("The field [" + name + "] cannot have custom similarities");
        }

        @Override
        public Builder index(boolean index) {
            if (index == false) {
                throw new MapperParsingException("The field [" + name + "] cannot have index = false");
            }
            return this;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }


        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setHasDocValues(true);
            fieldType().setTokenized(false);
            fieldType().setIndexOptions(IndexOptions.DOCS);
        }

        @Override
        public WildcardFieldType fieldType() {
            return (WildcardFieldType) super.fieldType();
        }

        @Override
        public WildcardFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new WildcardFieldMapper(
                    name, fieldType, defaultFieldType, ignoreAbove,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            WildcardFieldMapper.Builder builder = new WildcardFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                }
            }

            return builder;
        }
    }

     public static final char TOKEN_START_OR_END_CHAR = 0;
     public static final String TOKEN_START_STRING = Character.toString(TOKEN_START_OR_END_CHAR);
     public static final String TOKEN_END_STRING = TOKEN_START_STRING + TOKEN_START_STRING;

     public static final class WildcardFieldType extends MappedFieldType {
         
        static Analyzer lowercaseNormalizer = new LowercaseNormalizer();

        public WildcardFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected WildcardFieldType(WildcardFieldType ref) {
            super(ref);
        }

        public WildcardFieldType clone() {
            WildcardFieldType result = new WildcardFieldType(this);
            return result;
        }


        @Override
        public Query wildcardQuery(String wildcardPattern, RewriteMethod method, QueryShardContext context) {

            String ngramIndexPattern = addLineEndChars(toLowerCase(wildcardPattern));

            // Break search term into tokens
            Set<String> tokens = new LinkedHashSet<>();
            StringBuilder sequence = new StringBuilder();
            int numWildcardChars = 0;
            int numWildcardStrings = 0;
            for (int i = 0; i < ngramIndexPattern.length();) {
                final int c = ngramIndexPattern.codePointAt(i);
                int length = Character.charCount(c);
                switch (c) {
                    case WildcardQuery.WILDCARD_STRING:
                        if (sequence.length() > 0) {
                            getNgramTokens(tokens, sequence.toString());
                            sequence = new StringBuilder();
                        }
                        numWildcardStrings++;
                        break;
                    case WildcardQuery.WILDCARD_CHAR:
                        if (sequence.length() > 0) {
                            getNgramTokens(tokens, sequence.toString());
                            sequence = new StringBuilder();
                        }
                        numWildcardChars++;
                        break;
                    case WildcardQuery.WILDCARD_ESCAPE:
                        // add the next codepoint instead, if it exists
                        if (i + length < ngramIndexPattern.length()) {
                            final int nextChar = ngramIndexPattern.codePointAt(i + length);
                            length += Character.charCount(nextChar);
                            sequence.append(Character.toChars(nextChar));
                        } else {
                            sequence.append(Character.toChars(c));
                        }
                        break;

                    default:
                        sequence.append(Character.toChars(c));
                }
                i += length;
            }

            if (sequence.length() > 0) {
                getNgramTokens(tokens, sequence.toString());
            }

            BooleanQuery.Builder rewritten = new BooleanQuery.Builder();
            int clauseCount = 0;
            for (String string : tokens) {
                if (clauseCount >= MAX_CLAUSES_IN_APPROXIMATION_QUERY) {
                    break;
                }
                addClause(string, rewritten, Occur.MUST);
                clauseCount++;
            }
            Supplier<Automaton> deferredAutomatonSupplier = () -> {
                return WildcardQuery.toAutomaton(new Term(name(), wildcardPattern));
            };
            AutomatonQueryOnBinaryDv verifyingQuery = new AutomatonQueryOnBinaryDv(name(), wildcardPattern, deferredAutomatonSupplier);
            if (clauseCount > 0) {
                // We can accelerate execution with the ngram query
                BooleanQuery approxQuery = rewritten.build();
                BooleanQuery.Builder verifyingBuilder = new BooleanQuery.Builder();
                verifyingBuilder.add(new BooleanClause(approxQuery, Occur.MUST));
                verifyingBuilder.add(new BooleanClause(verifyingQuery, Occur.MUST));
                return verifyingBuilder.build();
            } else if (numWildcardChars == 0 || numWildcardStrings > 0) {
                // We have no concrete characters and we're not a pure length query e.g. ??? 
                return new DocValuesFieldExistsQuery(name());
            }
            return verifyingQuery;

        }
        
        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates, RewriteMethod method, QueryShardContext context) {
            if (value.length() == 0) {
                return new MatchNoDocsQuery();
            }

            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                );
            }
            
            RegExp ngramRegex = new RegExp(addLineEndChars(toLowerCase(value)), flags);

            Query approxBooleanQuery = toApproximationQuery(ngramRegex);
            Query approxNgramQuery = rewriteBoolToNgramQuery(approxBooleanQuery);
            
            // MatchAll is a special case meaning the regex is known to match everything .* and 
            // there is no need for verification.
            if (approxNgramQuery instanceof MatchAllDocsQuery) {
                return existsQuery(context);
            }
            Supplier<Automaton> deferredAutomatonSupplier = ()-> {
                RegExp regex = new RegExp(value, flags);
                return regex.toAutomaton(maxDeterminizedStates);
            };

            AutomatonQueryOnBinaryDv verifyingQuery = new AutomatonQueryOnBinaryDv(name(), value, deferredAutomatonSupplier);
            
            // MatchAllButRequireVerificationQuery is a special case meaning the regex is reduced to a single
            // clause which we can't accelerate at all and needs verification. Example would be ".." 
            if (approxNgramQuery instanceof MatchAllButRequireVerificationQuery) {
                return verifyingQuery;
            }
            
            // We can accelerate execution with the ngram query
            BooleanQuery.Builder verifyingBuilder = new BooleanQuery.Builder();
            verifyingBuilder.add(new BooleanClause(approxNgramQuery, Occur.MUST));
            verifyingBuilder.add(new BooleanClause(verifyingQuery, Occur.MUST));
            return verifyingBuilder.build();
        }
        
        // Convert a regular expression to a simplified query consisting of BooleanQuery and TermQuery objects
        // which captures as much of the logic as possible. Query can produce some false positives but shouldn't
        // produce any false negatives.
        // In addition to Term and BooleanQuery clauses there are MatchAllDocsQuery objects (e.g for .*) and
        // a RegExpQuery if we can't resolve to any of the above. 
        // *  If an expression resolves to a single MatchAllDocsQuery eg .* then a match all shortcut is possible with 
        //    no verification needed.     
        // * If an expression resolves to a RegExpQuery eg ?? then only the verification 
        //   query is run.
        // * Anything else is a concrete query that should be run on the ngram index.
        public static Query toApproximationQuery(RegExp r) throws IllegalArgumentException {
            Query result = null;
            switch (r.kind) {
                case REGEXP_UNION:
                    result = createUnionQuery(r);
                    break;
                case REGEXP_CONCATENATION:
                    result = createConcatenationQuery(r);
                    break;
                case REGEXP_STRING:
                    String normalizedString = toLowerCase(r.s);
                    result = new TermQuery(new Term("", normalizedString));
                    break;
                case REGEXP_CHAR:
                    String cs = Character.toString(r.c);
                    String normalizedChar = toLowerCase(cs);
                    result = new TermQuery(new Term("", normalizedChar));
                    break;
                case REGEXP_REPEAT:
                    // Repeat is zero or more times so zero matches = match all
                    result = new MatchAllDocsQuery();
                    break;
                    
                case REGEXP_REPEAT_MIN:
                case REGEXP_REPEAT_MINMAX:
                    if (r.min > 0) {
                        result = toApproximationQuery(r.exp1);
                        if(result instanceof TermQuery) {
                            // Wrap the repeating expression so that it is not concatenated by a parent which concatenates
                            // plain TermQuery objects together. Boolean queries are interpreted as a black box and not
                            // concatenated.
                            BooleanQuery.Builder wrapper = new BooleanQuery.Builder();
                            wrapper.add(result, Occur.MUST);
                            result = wrapper.build();
                        }
                    } else {
                        // Expressions like (a){0,3} match empty string or up to 3 a's.
                        result = new MatchAllButRequireVerificationQuery();
                    }
                    break;
                case REGEXP_ANYSTRING:
                    // optimisation for .* queries - match all and no verification stage required.
                    result = new MatchAllDocsQuery();
                    break;
                // All other kinds of expression cannot be represented as a boolean or term query so return an object 
                // that indicates verification is required
                case REGEXP_OPTIONAL:
                case REGEXP_INTERSECTION:
                case REGEXP_COMPLEMENT:
                case REGEXP_CHAR_RANGE:
                case REGEXP_ANYCHAR:
                case REGEXP_INTERVAL:
                case REGEXP_EMPTY: 
                case REGEXP_AUTOMATON:
                    result = new MatchAllButRequireVerificationQuery();
                    break;
            }
            assert result != null; // All regex types are understood and translated to a query.
            return result;
        }
        
        private static Query createConcatenationQuery(RegExp r) {
            // Create ANDs of expressions plus collapse consecutive TermQuerys into single longer ones
            ArrayList<Query> queries = new ArrayList<>();
            findLeaves(r.exp1, Kind.REGEXP_CONCATENATION, queries);
            findLeaves(r.exp2, Kind.REGEXP_CONCATENATION, queries);
            BooleanQuery.Builder bAnd = new BooleanQuery.Builder();
            StringBuilder sequence = new StringBuilder();
            for (Query query : queries) {
                if (query instanceof TermQuery) {
                    TermQuery tq = (TermQuery) query;
                    sequence.append(tq.getTerm().text());
                } else {
                    if (sequence.length() > 0) {
                        bAnd.add(new TermQuery(new Term("", sequence.toString())), Occur.MUST);
                        sequence = new StringBuilder();
                    }
                    bAnd.add(query, Occur.MUST);                    
                }
            }
            if (sequence.length() > 0) {
                bAnd.add(new TermQuery(new Term("", sequence.toString())), Occur.MUST);
            }
            BooleanQuery combined = bAnd.build();
            if (combined.clauses().size() > 0) {
                return combined;
            }
            // There's something in the regex we couldn't represent as a query - resort to a match all with verification 
            return new MatchAllButRequireVerificationQuery();
            
        }

        private static Query createUnionQuery(RegExp r) {
            // Create an OR of clauses
            ArrayList<Query> queries = new ArrayList<>();
            findLeaves(r.exp1, Kind.REGEXP_UNION, queries);
            findLeaves(r.exp2, Kind.REGEXP_UNION, queries);
            BooleanQuery.Builder bOr = new BooleanQuery.Builder();
            HashSet<Query> uniqueClauses = new HashSet<>();
            for (Query query : queries) {
                if (uniqueClauses.add(query)) {
                    bOr.add(query, Occur.SHOULD);
                }
            }
            if (uniqueClauses.size() > 0) {
                if (uniqueClauses.size() == 1) {
                    // Fully-understood ORs that collapse to a single term should be returned minus
                    // the BooleanQuery wrapper so that they might be concatenated.
                    // Helps turn [Pp][Oo][Ww][Ee][Rr][Ss][Hh][Ee][Ll][Ll] into "powershell"
                    // Each char pair eg (P OR p) can be normalized to (p) which can be a single term
                    return uniqueClauses.iterator().next();
                } else {
                    return bOr.build();
                }
            }
            // There's something in the regex we couldn't represent as a query - resort to a match all with verification 
            return new MatchAllButRequireVerificationQuery();
        }

        private static void findLeaves(RegExp exp, Kind kind, List<Query> queries) {
            if (exp.kind == kind) {
                findLeaves(exp.exp1, kind, queries);
                findLeaves( exp.exp2, kind, queries);
            } else {
                queries.add(toApproximationQuery(exp));
            }
        }        
        
        private static String toLowerCase(String string) {
            return lowercaseNormalizer.normalize(null, string).utf8ToString();
        }
        
        // Takes a BooleanQuery + TermQuery tree representing query logic and rewrites using ngrams of appropriate size.
        private Query rewriteBoolToNgramQuery(Query approxQuery) {
            //TODO optimise more intelligently so we: 
            // 1) favour full-length term queries eg abc over short eg a* when pruning too many clauses.
            // 2) make MAX_CLAUSES_IN_APPROXIMATION_QUERY a global cap rather than per-boolean clause.
            if (approxQuery == null) {
                return null;
            }
            if (approxQuery instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) approxQuery;
                BooleanQuery.Builder rewritten = new BooleanQuery.Builder();
                int clauseCount = 0;
                for (BooleanClause clause : bq) {
                    Query q = rewriteBoolToNgramQuery(clause.getQuery());
                    if (q != null) {
                        if (clause.getOccur().equals(Occur.MUST)) {
                            // Can't drop "should" clauses because it can elevate a sibling optional item
                            // to mandatory (shoulds with 1 clause) causing false negatives
                            // Dropping MUSTs increase false positives which are OK because are verified anyway.
                            clauseCount++;
                            if (clauseCount >= MAX_CLAUSES_IN_APPROXIMATION_QUERY) {
                                break;
                            }
                        }
                        rewritten.add(q, clause.getOccur());
                    }
                }
                return simplify(rewritten.build());
            }
            if (approxQuery instanceof TermQuery) {
                TermQuery tq = (TermQuery) approxQuery;
               
                //Remove simple terms that are only string beginnings or ends.
                String s = tq.getTerm().text();
                if (s.equals(WildcardFieldMapper.TOKEN_START_STRING) || s.equals(WildcardFieldMapper.TOKEN_END_STRING)) {
                    return new MatchAllButRequireVerificationQuery();
                }
                
                // Break term into tokens
                Set<String> tokens = new LinkedHashSet<>();
                getNgramTokens(tokens, s);
                BooleanQuery.Builder rewritten = new BooleanQuery.Builder();
                for (String string : tokens) {
                    addClause(string, rewritten, Occur.MUST);
                }
                return simplify(rewritten.build());
            }
            if (isMatchAll(approxQuery)) {
                return approxQuery;
            }
            throw new IllegalStateException("Invalid query type found parsing regex query:" + approxQuery);
        }    
        
        static Query simplify(Query input) {
            if (input instanceof BooleanQuery == false) {
                return input;
            }
            BooleanQuery result = (BooleanQuery) input;
            if (result.clauses().size() == 0) {
                // A ".*" clause can produce zero clauses in which case we return MatchAll
                return new MatchAllDocsQuery();
            }
            if (result.clauses().size() == 1) {
                return simplify(result.clauses().get(0).getQuery());
            }

            // We may have a mix of MatchAll and concrete queries - assess if we can simplify
            int matchAllCount = 0;
            int verifyCount = 0;
            boolean allConcretesAreOptional = true;
            for (BooleanClause booleanClause : result.clauses()) {
                Query q = booleanClause.getQuery();
                if (q instanceof MatchAllDocsQuery) {
                    matchAllCount++;
                } else if (q instanceof MatchAllButRequireVerificationQuery) {
                    verifyCount++;
                } else {
                    // Concrete query
                    if (booleanClause.getOccur() != Occur.SHOULD) {
                        allConcretesAreOptional = false;
                    }
                }
            }

            if ((allConcretesAreOptional && matchAllCount > 0)) {
                // Any match all expression takes precedence over all optional concrete queries.
                return new MatchAllDocsQuery();
            }

            if ((allConcretesAreOptional && verifyCount > 0)) {
                // Any match all expression that needs verification takes precedence over all optional concrete queries.
                return new MatchAllButRequireVerificationQuery();
            }

            // We have some mandatory concrete queries - strip out the superfluous match all expressions
            if (allConcretesAreOptional == false && matchAllCount + verifyCount > 0) {
                BooleanQuery.Builder rewritten = new BooleanQuery.Builder();
                for (BooleanClause booleanClause : result.clauses()) {
                    if (isMatchAll(booleanClause.getQuery()) == false) {
                        rewritten.add(booleanClause);
                    }
                }
                return simplify(rewritten.build());
            }
            return result;
        }
        
        
        static boolean isMatchAll(Query q) {
            return q instanceof MatchAllDocsQuery || q instanceof MatchAllButRequireVerificationQuery;
        }

        protected void getNgramTokens(Set<String> tokens, String fragment) {
            if (fragment.equals(TOKEN_START_STRING) || fragment.equals(TOKEN_END_STRING)) {
                // If a regex is a form of match-all e.g. ".*" we only produce the token start/end markers as search
                // terms which can be ignored.
                return;
            }
            // Break fragment into multiple Ngrams
            TokenStream tokenizer = WILDCARD_ANALYZER.tokenStream(name(), fragment);
            CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
            // If fragment length < NGRAM_SIZE then it is not emitted by token stream so need
            // to initialise with the value here
            String lastUnusedToken = fragment;
            try {
                tokenizer.reset();
                boolean takeThis = true;
                // minimise number of terms searched - eg for "12345" and 3grams we only need terms
                // `123` and `345` - no need to search for 234. We take every other ngram.
                while (tokenizer.incrementToken()) {
                    String tokenValue = termAtt.toString();
                    if (takeThis) {
                        tokens.add(tokenValue);
                        lastUnusedToken = null;
                    } else {
                        lastUnusedToken = tokenValue;
                    }
                    // alternate
                    takeThis = !takeThis;
                    if (tokens.size() >= MAX_CLAUSES_IN_APPROXIMATION_QUERY) {
                        lastUnusedToken = null;
                        break;
                    }
                }
                if (lastUnusedToken != null) {
                    // given `cake` and 3 grams the loop above would output only `cak` and we need to add trailing
                    // `ake` to complete the logic.
                    tokens.add(lastUnusedToken);
                }
                tokenizer.end();
                tokenizer.close();
            } catch (IOException ioe) {
                throw new ElasticsearchParseException("Error parsing wildcard regex pattern fragment [" + fragment + "]");
            }
        }
        

        private void addClause(String token, BooleanQuery.Builder bqBuilder, Occur occur) {
            assert token.codePointCount(0, token.length()) <= NGRAM_SIZE;
            int tokenSize = token.codePointCount(0, token.length());
            if (tokenSize < 2 || token.equals(WildcardFieldMapper.TOKEN_END_STRING)) {
                // there's something concrete to be searched but it's too short
                // Require verification.
                bqBuilder.add(new BooleanClause(new MatchAllButRequireVerificationQuery(), occur));
                return;
            }
            if (tokenSize == NGRAM_SIZE) {
                TermQuery tq = new TermQuery(new Term(name(), token));
                bqBuilder.add(new BooleanClause(tq, occur));
            } else {
                PrefixQuery wq = new PrefixQuery(new Term(name(), token));
                wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
                bqBuilder.add(new BooleanClause(wq, occur));
            }
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            QueryShardContext context
        ) {
            String searchTerm = BytesRefs.toString(value);
            String lowerSearchTerm = toLowerCase(searchTerm);
            try {
                BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
                //The approximation query can have a prefix and any number of ngrams.
                BooleanQuery.Builder approxBuilder = new BooleanQuery.Builder();
                
                String postPrefixString = lowerSearchTerm;
                
                // Add all content prior to prefixLength as a MUST clause to the ngram index query
                if (prefixLength > 0) {
                    Set<String> prefixTokens = new LinkedHashSet<>();
                    postPrefixString = lowerSearchTerm.substring(prefixLength);
                    String prefixCandidate = TOKEN_START_OR_END_CHAR + lowerSearchTerm.substring(0,  prefixLength);
                    getNgramTokens(prefixTokens, prefixCandidate);
                    for (String prefixToken : prefixTokens) {
                        addClause(prefixToken, approxBuilder, Occur.MUST);
                    }
                }
                // Tokenize all content after the prefix
                TokenStream tokenizer = WILDCARD_ANALYZER.tokenStream(name(), postPrefixString);
                CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
                ArrayList<String> postPrefixTokens = new ArrayList<>();
                String firstToken = null;
                tokenizer.reset();
                int tokenNumber = 0;
                while (tokenizer.incrementToken()) {
                    if (tokenNumber == 0) {
                        String token = termAtt.toString();
                        if (firstToken == null) {
                            firstToken = token;
                        }
                        postPrefixTokens.add(token);
                    }
                    // Take every 3rd ngram so they are all disjoint. Our calculation for min_should_match
                    // number relies on there being no overlaps
                    tokenNumber++;
                    if (tokenNumber == 3) {
                        tokenNumber = 0;
                    }
                }
                tokenizer.end();
                tokenizer.close();
                
                BooleanQuery.Builder ngramBuilder = new BooleanQuery.Builder();
                int numClauses = 0;
                for (String token : postPrefixTokens) {
                    addClause(token, ngramBuilder, Occur.SHOULD);
                    numClauses++;
                }

                // Approximation query
                if (numClauses > fuzziness.asDistance(searchTerm)) {
                    // Useful accelerant - set min should match based on number of permitted edits.
                    ngramBuilder.setMinimumNumberShouldMatch(numClauses - fuzziness.asDistance(searchTerm));
                    approxBuilder.add(ngramBuilder.build(), Occur.MUST);
                }
                
                BooleanQuery ngramQ = approxBuilder.build();
                if (ngramQ.clauses().size()>0) {
                    bqBuilder.add(ngramQ, Occur.MUST);
                }

                Supplier <Automaton> deferredAutomatonSupplier = ()->{
                    // Verification query
                    FuzzyQuery fq = new FuzzyQuery(
                        new Term(name(), searchTerm),
                        fuzziness.asDistance(searchTerm),
                        prefixLength,
                        maxExpansions,
                        transpositions
                    );
                    return fq.getAutomata().automaton;
                };
                bqBuilder.add(new AutomatonQueryOnBinaryDv(name(), searchTerm, deferredAutomatonSupplier), Occur.MUST);

                return bqBuilder.build();
            } catch (IOException ioe) {
                throw new ElasticsearchParseException("Error parsing wildcard field fuzzy string [" + searchTerm + "]");
            }
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return wildcardQuery(BytesRefs.toString(value), MultiTermQuery.CONSTANT_SCORE_REWRITE, context);
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            return wildcardQuery(value + "*", method, context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            for (Object value : values) {
                bq.add(termQuery(value, context), Occur.SHOULD);
            }
            return new ConstantScoreQuery(bq.build());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new IndexFieldData.Builder() {

                @Override
                public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                        CircuitBreakerService breakerService, MapperService mapperService) {
                    return new WildcardBytesBinaryIndexFieldData(indexSettings.getIndex(), fieldType.name());
                }};
        }

         @Override
         public ValuesSourceType getValuesSourceType() {
             return CoreValuesSourceType.BYTES;
         }

    }

    static class WildcardBytesBinaryIndexFieldData extends BytesBinaryIndexFieldData {

        WildcardBytesBinaryIndexFieldData(Index index, String fieldName) {
            super(index, fieldName);
        }

        @Override
        public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue,
                    sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

    }

    private int ignoreAbove;

    private WildcardFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                int ignoreAbove, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreAbove = ignoreAbove;
        assert fieldType.indexOptions() == IndexOptions.DOCS;

        ngramFieldType = fieldType.clone();
        ngramFieldType.setTokenized(true);
        ngramFieldType.freeze();
    }

    /** Values that have more chars than the return value of this method will
     *  be skipped at parsing time. */
    // pkg-private for testing
    int ignoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected WildcardFieldMapper clone() {
        return (WildcardFieldMapper) super.clone();
    }

    @Override
    public WildcardFieldType fieldType() {
        return (WildcardFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = fieldType().nullValueAsString();
            } else {
                value =  parser.textOrNull();
            }
        }
        ParseContext.Document parseDoc = context.doc();

        List<IndexableField> fields = new ArrayList<>();
        createFields(value, parseDoc, fields);
        parseDoc.addAll(fields);
    }

    @Override
    protected String parseSourceValue(Object value) {
        return value.toString();
    }

    // For internal use by Lucene only - used to define ngram index
    final MappedFieldType ngramFieldType;

    void createFields(String value, Document parseDoc, List<IndexableField>fields) throws IOException {
        if (value == null || value.length() > ignoreAbove) {
            return;
        }
        // Always lower case the ngram index and value - helps with 
        // a) speed (less ngram variations to explore on disk and in RAM-based automaton) and 
        // b) uses less disk space
        String ngramValue = addLineEndChars(WildcardFieldType.toLowerCase(value));
        Field ngramField = new Field(fieldType().name(), ngramValue, ngramFieldType);
        fields.add(ngramField);

        CustomBinaryDocValuesField dvField = (CustomBinaryDocValuesField) parseDoc.getByKey(fieldType().name());
        if (dvField == null) {
            dvField = new CustomBinaryDocValuesField(fieldType().name(), value.getBytes(StandardCharsets.UTF_8));
            parseDoc.addWithKey(fieldType().name(), dvField);
        } else {
            dvField.add(value.getBytes(StandardCharsets.UTF_8));
        }
    }
    
    // Values held in the ngram index are encoded with special characters to denote start and end of values.
    static String addLineEndChars(String value) {
        return TOKEN_START_OR_END_CHAR + value + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        this.ignoreAbove = ((WildcardFieldMapper) other).ignoreAbove;
    }
}
