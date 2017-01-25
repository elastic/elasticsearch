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

package org.apache.lucene.queryparser.classic;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.analyzing.AnalyzingQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.GraphQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query parser that uses the {@link MapperService} in order to build smarter
 * queries based on the mapping information.
 * <p>
 * Also breaks fields with [type].[name] into a boolean query that must include the type
 * as well as the query on the name.
 */
public class MapperQueryParser extends AnalyzingQueryParser {

    public static final Map<String, FieldQueryExtension> FIELD_QUERY_EXTENSIONS;

    static {
        Map<String, FieldQueryExtension> fieldQueryExtensions = new HashMap<>();
        fieldQueryExtensions.put(ExistsFieldQueryExtension.NAME, new ExistsFieldQueryExtension());
        FIELD_QUERY_EXTENSIONS = unmodifiableMap(fieldQueryExtensions);
    }

    private final QueryShardContext context;

    private QueryParserSettings settings;

    private MappedFieldType currentFieldType;

    public MapperQueryParser(QueryShardContext context) {
        super(null, null);
        this.context = context;
    }

    public void reset(QueryParserSettings settings) {
        this.settings = settings;
        if (settings.fieldsAndWeights().isEmpty()) {
            this.field = settings.defaultField();
        } else if (settings.fieldsAndWeights().size() == 1) {
            this.field = settings.fieldsAndWeights().keySet().iterator().next();
        } else {
            this.field = null;
        }
        setAnalyzer(settings.analyzer());
        setMultiTermRewriteMethod(settings.rewriteMethod());
        setEnablePositionIncrements(settings.enablePositionIncrements());
        setAutoGeneratePhraseQueries(settings.autoGeneratePhraseQueries());
        setMaxDeterminizedStates(settings.maxDeterminizedStates());
        setAllowLeadingWildcard(settings.allowLeadingWildcard());
        setLowercaseExpandedTerms(false);
        setPhraseSlop(settings.phraseSlop());
        setDefaultOperator(settings.defaultOperator());
        setFuzzyPrefixLength(settings.fuzzyPrefixLength());
        setSplitOnWhitespace(settings.splitOnWhitespace());
    }

    /**
     * We override this one so we can get the fuzzy part to be treated as string,
     * so people can do: "age:10~5" or "timestamp:2012-10-10~5d"
     */
    @Override
    Query handleBareFuzzy(String qfield, Token fuzzySlop, String termImage) throws ParseException {
        if (fuzzySlop.image.length() == 1) {
            return getFuzzyQuery(qfield, termImage, Float.toString(settings.fuzziness().asDistance(termImage)));
        }
        return getFuzzyQuery(qfield, termImage, fuzzySlop.image.substring(1));
    }

    @Override
    protected Query newTermQuery(Term term) {
        if (currentFieldType != null) {
            Query termQuery = currentFieldType.queryStringTermQuery(term);
            if (termQuery != null) {
                return termQuery;
            }
        }
        return super.newTermQuery(term);
    }

    @Override
    protected Query newMatchAllDocsQuery() {
        return Queries.newMatchAllQuery();
    }

    @Override
    public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
        FieldQueryExtension fieldQueryExtension = FIELD_QUERY_EXTENSIONS.get(field);
        if (fieldQueryExtension != null) {
            return fieldQueryExtension.query(context, queryText);
        }
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (fields.size() == 1) {
                return getFieldQuerySingle(fields.iterator().next(), queryText, quoted);
            }
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = getFieldQuerySingle(mField, queryText, quoted);
                    if (q != null) {
                        added = true;
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = getFieldQuerySingle(mField, queryText, quoted);
                    if (q != null) {
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                if (clauses.isEmpty()) return null; // happens for stopwords
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return getFieldQuerySingle(field, queryText, quoted);
        }
    }

    private Query getFieldQuerySingle(String field, String queryText, boolean quoted) throws ParseException {
        if (!quoted && queryText.length() > 1) {
            if (queryText.charAt(0) == '>') {
                if (queryText.length() > 2) {
                    if (queryText.charAt(1) == '=') {
                        return getRangeQuerySingle(field, queryText.substring(2), null, true, true, context);
                    }
                }
                return getRangeQuerySingle(field, queryText.substring(1), null, false, true, context);
            } else if (queryText.charAt(0) == '<') {
                if (queryText.length() > 2) {
                    if (queryText.charAt(1) == '=') {
                        return getRangeQuerySingle(field, null, queryText.substring(2), true, true, context);
                    }
                }
                return getRangeQuerySingle(field, null, queryText.substring(1), true, false, context);
            }
        }
        currentFieldType = null;
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            if (quoted) {
                setAnalyzer(settings.quoteAnalyzer());
                if (settings.quoteFieldSuffix() != null) {
                    currentFieldType = context.fieldMapper(field + settings.quoteFieldSuffix());
                }
            }
            if (currentFieldType == null) {
                currentFieldType = context.fieldMapper(field);
            }
            if (currentFieldType != null) {
                if (quoted) {
                    if (!settings.forceQuoteAnalyzer()) {
                        setAnalyzer(context.getSearchQuoteAnalyzer(currentFieldType));
                    }
                } else {
                    if (!settings.forceAnalyzer()) {
                        setAnalyzer(context.getSearchAnalyzer(currentFieldType));
                    }
                }
                if (currentFieldType != null) {
                    Query query = null;
                    if (currentFieldType.tokenized() == false) {
                        // this might be a structured field like a numeric
                        try {
                            query = currentFieldType.termQuery(queryText, context);
                        } catch (RuntimeException e) {
                            if (settings.lenient()) {
                                return null;
                            } else {
                                throw e;
                            }
                        }
                    }
                    if (query == null) {
                        query = super.getFieldQuery(currentFieldType.name(), queryText, quoted);
                    }
                    return query;
                }
            }
            return super.getFieldQuery(field, queryText, quoted);
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = super.getFieldQuery(mField, queryText, slop);
                    if (q != null) {
                        added = true;
                        q = applySlop(q, slop);
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = super.getFieldQuery(mField, queryText, slop);
                    if (q != null) {
                        q = applySlop(q, slop);
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                if (clauses.isEmpty()) return null; // happens for stopwords
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return super.getFieldQuery(field, queryText, slop);
        }
    }

    @Override
    protected Query getRangeQuery(String field, String part1, String part2,
                                  boolean startInclusive, boolean endInclusive) throws ParseException {
        if ("*".equals(part1)) {
            part1 = null;
        }
        if ("*".equals(part2)) {
            part2 = null;
        }

        Collection<String> fields = extractMultiFields(field);

        if (fields == null) {
            return getRangeQuerySingle(field, part1, part2, startInclusive, endInclusive, context);
        }


        if (fields.size() == 1) {
            return getRangeQuerySingle(fields.iterator().next(), part1, part2, startInclusive, endInclusive, context);
        }

        if (settings.useDisMax()) {
            List<Query> queries = new ArrayList<>();
            boolean added = false;
            for (String mField : fields) {
                Query q = getRangeQuerySingle(mField, part1, part2, startInclusive, endInclusive, context);
                if (q != null) {
                    added = true;
                    queries.add(applyBoost(mField, q));
                }
            }
            if (!added) {
                return null;
            }
            return new DisjunctionMaxQuery(queries, settings.tieBreaker());
        } else {
            List<BooleanClause> clauses = new ArrayList<>();
            for (String mField : fields) {
                Query q = getRangeQuerySingle(mField, part1, part2, startInclusive, endInclusive, context);
                if (q != null) {
                    clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                }
            }
            if (clauses.isEmpty()) return null; // happens for stopwords
            return getBooleanQueryCoordDisabled(clauses);
        }
    }

    private Query getRangeQuerySingle(String field, String part1, String part2,
            boolean startInclusive, boolean endInclusive, QueryShardContext context) {
        currentFieldType = context.fieldMapper(field);
        if (currentFieldType != null) {
            try {
                BytesRef part1Binary = part1 == null ? null : getAnalyzer().normalize(field, part1);
                BytesRef part2Binary = part2 == null ? null : getAnalyzer().normalize(field, part2);
                Query rangeQuery;
                if (currentFieldType instanceof DateFieldMapper.DateFieldType && settings.timeZone() != null) {
                    DateFieldMapper.DateFieldType dateFieldType = (DateFieldMapper.DateFieldType) this.currentFieldType;
                    rangeQuery = dateFieldType.rangeQuery(part1Binary, part2Binary,
                            startInclusive, endInclusive, settings.timeZone(), null, context);
                } else {
                    rangeQuery = currentFieldType.rangeQuery(part1Binary, part2Binary, startInclusive, endInclusive, context);
                }
                return rangeQuery;
            } catch (RuntimeException e) {
                if (settings.lenient()) {
                    return null;
                }
                throw e;
            }
        }
        return newRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }

    protected Query getFuzzyQuery(String field, String termStr, String minSimilarity) throws ParseException {
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (fields.size() == 1) {
                return getFuzzyQuerySingle(fields.iterator().next(), termStr, minSimilarity);
            }
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = getFuzzyQuerySingle(mField, termStr, minSimilarity);
                    if (q != null) {
                        added = true;
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = getFuzzyQuerySingle(mField, termStr, minSimilarity);
                    if (q != null) {
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return getFuzzyQuerySingle(field, termStr, minSimilarity);
        }
    }

    private Query getFuzzyQuerySingle(String field, String termStr, String minSimilarity) throws ParseException {
        currentFieldType = context.fieldMapper(field);
        if (currentFieldType != null) {
            try {
                BytesRef term = termStr == null ? null : getAnalyzer().normalize(field, termStr);
                return currentFieldType.fuzzyQuery(term, Fuzziness.build(minSimilarity),
                    getFuzzyPrefixLength(), settings.fuzzyMaxExpansions(), FuzzyQuery.defaultTranspositions);
            } catch (RuntimeException e) {
                if (settings.lenient()) {
                    return null;
                }
                throw e;
            }
        }
        return super.getFuzzyQuery(field, termStr, Float.parseFloat(minSimilarity));
    }

    @Override
    protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
        String text = term.text();
        int numEdits = FuzzyQuery.floatToEdits(minimumSimilarity, text.codePointCount(0, text.length()));
        FuzzyQuery query = new FuzzyQuery(term, numEdits, prefixLength,
            settings.fuzzyMaxExpansions(), FuzzyQuery.defaultTranspositions);
        QueryParsers.setRewriteMethod(query, settings.fuzzyRewriteMethod());
        return query;
    }

    @Override
    protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (fields.size() == 1) {
                return getPrefixQuerySingle(fields.iterator().next(), termStr);
            }
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = getPrefixQuerySingle(mField, termStr);
                    if (q != null) {
                        added = true;
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = getPrefixQuerySingle(mField, termStr);
                    if (q != null) {
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                if (clauses.isEmpty()) return null; // happens for stopwords
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return getPrefixQuerySingle(field, termStr);
        }
    }

    private Query getPrefixQuerySingle(String field, String termStr) throws ParseException {
        currentFieldType = null;
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            currentFieldType = context.fieldMapper(field);
            if (currentFieldType != null) {
                if (!settings.forceAnalyzer()) {
                    setAnalyzer(context.getSearchAnalyzer(currentFieldType));
                }
                Query query = null;
                if (currentFieldType instanceof StringFieldType == false) {
                    query = currentFieldType.prefixQuery(termStr, getMultiTermRewriteMethod(), context);
                }
                if (query == null) {
                    query = getPossiblyAnalyzedPrefixQuery(currentFieldType.name(), termStr);
                }
                return query;
            }
            return getPossiblyAnalyzedPrefixQuery(field, termStr);
        } catch (RuntimeException e) {
            if (settings.lenient()) {
                return null;
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    private Query getPossiblyAnalyzedPrefixQuery(String field, String termStr) throws ParseException {
        if (!settings.analyzeWildcard()) {
            return super.getPrefixQuery(field, termStr);
        }
        List<List<String> > tlist;
        // get Analyzer from superclass and tokenize the term
        TokenStream source = null;
        try {
            try {
                source = getAnalyzer().tokenStream(field, termStr);
                source.reset();
            } catch (IOException e) {
                return super.getPrefixQuery(field, termStr);
            }
            tlist = new ArrayList<>();
            List<String> currentPos = new ArrayList<>();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posAtt = source.addAttribute(PositionIncrementAttribute.class);

            while (true) {
                try {
                    if (!source.incrementToken()) break;
                } catch (IOException e) {
                    break;
                }
                if (currentPos.isEmpty() == false && posAtt.getPositionIncrement() > 0) {
                    tlist.add(currentPos);
                    currentPos = new ArrayList<>();
                }
                currentPos.add(termAtt.toString());
            }
            if (currentPos.isEmpty() == false) {
                tlist.add(currentPos);
            }
        } finally {
            if (source != null) {
                IOUtils.closeWhileHandlingException(source);
            }
        }

        if (tlist.size() == 0) {
            return null;
        }

        if (tlist.size() == 1 && tlist.get(0).size() == 1) {
            return super.getPrefixQuery(field, tlist.get(0).get(0));
        }

        // build a boolean query with prefix on the last position only.
        List<BooleanClause> clauses = new ArrayList<>();
        for (int pos = 0; pos < tlist.size(); pos++) {
            List<String> plist = tlist.get(pos);
            boolean isLastPos = (pos == tlist.size() - 1);
            Query posQuery;
            if (plist.size() == 1) {
                if (isLastPos) {
                    posQuery = super.getPrefixQuery(field, plist.get(0));
                } else {
                    posQuery = newTermQuery(new Term(field, plist.get(0)));
                }
            } else if (isLastPos == false) {
                // build a synonym query for terms in the same position.
                Term[] terms = new Term[plist.size()];
                for (int i = 0; i < plist.size(); i++) {
                    terms[i] = new Term(field, plist.get(i));
                }
                posQuery = new SynonymQuery(terms);
            } else {
                List<BooleanClause> innerClauses = new ArrayList<>();
                for (String token : plist) {
                    innerClauses.add(new BooleanClause(super.getPrefixQuery(field, token),
                        BooleanClause.Occur.SHOULD));
                }
                posQuery = getBooleanQueryCoordDisabled(innerClauses);
            }
            clauses.add(new BooleanClause(posQuery,
                getDefaultOperator() == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD));
        }
        return getBooleanQuery(clauses);
    }

    @Override
    protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        if (termStr.equals("*")) {
            // we want to optimize for match all query for the "*:*", and "*" cases
            if ("*".equals(field) || Objects.equals(field, this.field)) {
                String actualField = field;
                if (actualField == null) {
                    actualField = this.field;
                }
                if (actualField == null) {
                    return newMatchAllDocsQuery();
                }
                if ("*".equals(actualField) || "_all".equals(actualField)) {
                    return newMatchAllDocsQuery();
                }
                // effectively, we check if a field exists or not
                return FIELD_QUERY_EXTENSIONS.get(ExistsFieldQueryExtension.NAME).query(context, actualField);
            }
        }
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (fields.size() == 1) {
                return getWildcardQuerySingle(fields.iterator().next(), termStr);
            }
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = getWildcardQuerySingle(mField, termStr);
                    if (q != null) {
                        added = true;
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = getWildcardQuerySingle(mField, termStr);
                    if (q != null) {
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                if (clauses.isEmpty()) return null; // happens for stopwords
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return getWildcardQuerySingle(field, termStr);
        }
    }

    private Query getWildcardQuerySingle(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        currentFieldType = null;
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            currentFieldType = context.fieldMapper(field);
            if (currentFieldType != null) {
                if (!settings.forceAnalyzer()) {
                    setAnalyzer(context.getSearchAnalyzer(currentFieldType));
                }
                indexedNameField = currentFieldType.name();
            }
            return super.getWildcardQuery(indexedNameField, termStr);
        } catch (RuntimeException e) {
            if (settings.lenient()) {
                return null;
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    @Override
    protected Query getRegexpQuery(String field, String termStr) throws ParseException {
        Collection<String> fields = extractMultiFields(field);
        if (fields != null) {
            if (fields.size() == 1) {
                return getRegexpQuerySingle(fields.iterator().next(), termStr);
            }
            if (settings.useDisMax()) {
                List<Query> queries = new ArrayList<>();
                boolean added = false;
                for (String mField : fields) {
                    Query q = getRegexpQuerySingle(mField, termStr);
                    if (q != null) {
                        added = true;
                        queries.add(applyBoost(mField, q));
                    }
                }
                if (!added) {
                    return null;
                }
                return new DisjunctionMaxQuery(queries, settings.tieBreaker());
            } else {
                List<BooleanClause> clauses = new ArrayList<>();
                for (String mField : fields) {
                    Query q = getRegexpQuerySingle(mField, termStr);
                    if (q != null) {
                        clauses.add(new BooleanClause(applyBoost(mField, q), BooleanClause.Occur.SHOULD));
                    }
                }
                if (clauses.isEmpty()) return null; // happens for stopwords
                return getBooleanQueryCoordDisabled(clauses);
            }
        } else {
            return getRegexpQuerySingle(field, termStr);
        }
    }

    private Query getRegexpQuerySingle(String field, String termStr) throws ParseException {
        currentFieldType = null;
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            currentFieldType = context.fieldMapper(field);
            if (currentFieldType != null) {
                if (!settings.forceAnalyzer()) {
                    setAnalyzer(context.getSearchAnalyzer(currentFieldType));
                }
                Query query = null;
                if (currentFieldType.tokenized() == false) {
                    query = currentFieldType.regexpQuery(termStr, RegExp.ALL,
                        getMaxDeterminizedStates(), getMultiTermRewriteMethod(), context);
                }
                if (query == null) {
                    query = super.getRegexpQuery(field, termStr);
                }
                return query;
            }
            return super.getRegexpQuery(field, termStr);
        } catch (RuntimeException e) {
            if (settings.lenient()) {
                return null;
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    /**
     * @deprecated review all use of this, don't rely on coord
     */
    @Deprecated
    protected Query getBooleanQueryCoordDisabled(List<BooleanClause> clauses) throws ParseException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setDisableCoord(true);
        for (BooleanClause clause : clauses) {
            builder.add(clause);
        }
        return fixNegativeQueryIfNeeded(builder.build());
    }


    @Override
    protected Query getBooleanQuery(List<BooleanClause> clauses) throws ParseException {
        Query q = super.getBooleanQuery(clauses);
        if (q == null) {
            return null;
        }
        return fixNegativeQueryIfNeeded(q);
    }

    private Query applyBoost(String field, Query q) {
        Float fieldBoost = settings.fieldsAndWeights().get(field);
        if (fieldBoost != null && fieldBoost != 1f) {
            return new BoostQuery(q, fieldBoost);
        }
        return q;
    }

    private Query applySlop(Query q, int slop) {
        if (q instanceof PhraseQuery) {
            //make sure that the boost hasn't been set beforehand, otherwise we'd lose it
            assert q instanceof BoostQuery == false;
            return addSlopToPhrase((PhraseQuery) q, slop);
        } else if (q instanceof MultiPhraseQuery) {
            MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder((MultiPhraseQuery) q);
            builder.setSlop(slop);
            return builder.build();
        } else if (q instanceof GraphQuery && ((GraphQuery) q).hasPhrase()) {
            // we have a graph query that has at least one phrase sub-query
            // re-build and set slop on all phrase queries
            List<Query> oldQueries = ((GraphQuery) q).getQueries();
            Query[] queries = new Query[oldQueries.size()];
            for (int i = 0; i < queries.length; i++) {
                Query oldQuery = oldQueries.get(i);
                if (oldQuery instanceof PhraseQuery) {
                    queries[i] = addSlopToPhrase((PhraseQuery) oldQuery, slop);
                } else {
                    queries[i] = oldQuery;
                }
            }

            return new GraphQuery(queries);
        } else {
            return q;
        }
    }

    /**
     * Rebuild a phrase query with a slop value
     */
    private PhraseQuery addSlopToPhrase(PhraseQuery query, int slop) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        final Term[] terms = query.getTerms();
        final int[] positions = query.getPositions();
        for (int i = 0; i < terms.length; ++i) {
            builder.add(terms[i], positions[i]);
        }

        return builder.build();
    }

    private Collection<String> extractMultiFields(String field) {
        Collection<String> fields;
        if (field != null) {
            fields = context.simpleMatchToIndexNames(field);
        } else {
            fields = settings.fieldsAndWeights().keySet();
        }
        return fields;
    }

    @Override
    public Query parse(String query) throws ParseException {
        if (query.trim().isEmpty()) {
            // if the query string is empty we return no docs / empty result
            // the behavior is simple to change in the client if all docs is required
            // or a default query
            return new MatchNoDocsQuery();
        }
        return super.parse(query);
    }
}
