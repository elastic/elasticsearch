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

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

public class MatchQuery {

    public static enum Type {
        BOOLEAN,
        PHRASE,
        PHRASE_PREFIX
    }

    public static enum ZeroTermsQuery {
        NONE,
        ALL
    }

    protected final QueryParseContext parseContext;

    protected String analyzer;

    protected BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    protected boolean enablePositionIncrements = true;

    protected int phraseSlop = 0;

    protected String maxEdits = null;
    protected int prefixLength = FuzzyQuery.defaultPrefixLength;
    protected int maxExpansions = FuzzyQuery.defaultMaxExpansions;
    //LUCENE 4 UPGRADE we need a default value for this!
    protected boolean transpositions = false;


    protected MultiTermQuery.RewriteMethod rewriteMethod;
    protected MultiTermQuery.RewriteMethod fuzzyRewriteMethod;

    protected boolean lenient;

    protected ZeroTermsQuery zeroTermsQuery = ZeroTermsQuery.NONE;

    public MatchQuery(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public void setOccur(BooleanClause.Occur occur) {
        this.occur = occur;
    }

    public void setEnablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
    }

    public void setPhraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
    }

    /**
     * Sets the maximum edit distance for fuzzy query
     * @deprecated use {@link #setMaxEdits(String)} instead;
     */
    @Deprecated
    public void setFuzziness(String maxEdits) {
        setMaxEdits(maxEdits);
    }
    
    /**
     * Sets the maximum edit distance for fuzzy query
     */
    public void setMaxEdits(String maxEdits) {
        this.maxEdits = maxEdits;
    }

    /**
     * Sets the constant prefix length for fuzzy query.
     */
    public void setPrefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
    }
    
    /**
     * Sets the constant prefix length for fuzzy query.
     * @deprecated use {@link #setPrefixLength(int)} instead
     */
    @Deprecated
    public void setFuzzyPrefixLength(int prefixLength) {
        setPrefixLength(prefixLength);
    }

    public void setMaxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
    }

    public void setTranspositions(boolean transpositions) {
        this.transpositions = transpositions;
    }

    public void setRewriteMethod(MultiTermQuery.RewriteMethod rewriteMethod) {
        this.rewriteMethod = rewriteMethod;
    }

    public void setFuzzyRewriteMethod(MultiTermQuery.RewriteMethod fuzzyRewriteMethod) {
        this.fuzzyRewriteMethod = fuzzyRewriteMethod;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public void setZeroTermsQuery(ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
    }

    public Query parse(Type type, String fieldName, Object value) {
        FieldMapper mapper = null;
        final String field;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            field = mapper.names().indexName();
        } else {
            field = fieldName;
        }

        if (mapper != null && mapper.useTermQueryWithQueryString()) {
            if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
                try {
                    return wrapSmartNameQuery(mapper.termQuery(value, parseContext), smartNameFieldMappers, parseContext);
                } catch (RuntimeException e) {
                    if (lenient) {
                        return null;
                    }
                    throw e;
                } finally {
                    QueryParseContext.setTypes(previousTypes);
                }
            } else {
                try {
                    return wrapSmartNameQuery(mapper.termQuery(value, parseContext), smartNameFieldMappers, parseContext);
                } catch (RuntimeException e) {
                    if (lenient) {
                        return null;
                    }
                    throw e;
                }
            }
        }

        Analyzer analyzer = null;
        if (this.analyzer == null) {
            if (mapper != null) {
                analyzer = mapper.searchAnalyzer();
            }
            if (analyzer == null && smartNameFieldMappers != null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
            if (analyzer == null) {
                analyzer = parseContext.mapperService().searchAnalyzer();
            }
        } else {
            analyzer = parseContext.mapperService().analysisService().analyzer(this.analyzer);
            if (analyzer == null) {
                throw new ElasticSearchIllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
            }
        }

        // Logic similar to QueryParser#getFieldQuery
        TokenStream source = null;
        CachingTokenFilter buffer = null;
        CharTermAttribute termAtt = null;
        PositionIncrementAttribute posIncrAtt = null;
        boolean success = false;
        try {
            source = analyzer.tokenStream(field, new FastStringReader(value.toString()));
            source.reset();
            success = true;
        } catch (IOException ex) {
            //LUCENE 4 UPGRADE not sure what todo here really lucene 3.6 had a tokenStream that didn't throw an exc.
            // success==false if we hit an exception
        }
        int numTokens = 0;
        int positionCount = 0;
        boolean severalTokensAtSamePosition = false;

        if (success) {
            buffer = new CachingTokenFilter(source);
            buffer.reset();
            if (buffer.hasAttribute(CharTermAttribute.class)) {
                termAtt = buffer.getAttribute(CharTermAttribute.class);
            }
            if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
                posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
            }

            boolean hasMoreTokens = false;
            if (termAtt != null) {
                try {
                    hasMoreTokens = buffer.incrementToken();
                    while (hasMoreTokens) {
                        numTokens++;
                        int positionIncrement = (posIncrAtt != null) ? posIncrAtt.getPositionIncrement() : 1;
                        if (positionIncrement != 0) {
                            positionCount += positionIncrement;
                        } else {
                            severalTokensAtSamePosition = true;
                        }
                        hasMoreTokens = buffer.incrementToken();
                    }
                } catch (IOException e) {
                    // ignore
                }
            }
            // rewind the buffer stream
            buffer.reset();
        }
        try {
            // close original stream - all tokens buffered
            if (source != null) {
                source.close();
            }
        } catch (IOException e) {
            // ignore
        }

        if (numTokens == 0) {
            return zeroTermsQuery();
        } else if (type == Type.BOOLEAN) {
            if (numTokens == 1) {
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }
                //LUCENE 4 UPGRADE instead of string term we can convert directly from utf-16 to utf-8
                Query q = newTermQuery(mapper, new Term(field, termToByteRef(termAtt, new BytesRef())));
                return wrapSmartNameQuery(q, smartNameFieldMappers, parseContext);
            }
            BooleanQuery q = new BooleanQuery(positionCount == 1);
            for (int i = 0; i < numTokens; i++) {
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }
                //LUCENE 4 UPGRADE instead of string term we can convert directly from utf-16 to utf-8
                Query currentQuery = newTermQuery(mapper, new Term(field, termToByteRef(termAtt, new BytesRef())));
                q.add(currentQuery, occur);
            }
            return wrapSmartNameQuery(q, smartNameFieldMappers, parseContext);
        } else if (type == Type.PHRASE) {
            if (severalTokensAtSamePosition) {
                MultiPhraseQuery mpq = new MultiPhraseQuery();
                mpq.setSlop(phraseSlop);
                List<Term> multiTerms = new ArrayList<Term>();
                int position = -1;
                for (int i = 0; i < numTokens; i++) {
                    int positionIncrement = 1;
                    try {
                        boolean hasNext = buffer.incrementToken();
                        assert hasNext == true;
                        if (posIncrAtt != null) {
                            positionIncrement = posIncrAtt.getPositionIncrement();
                        }
                    } catch (IOException e) {
                        // safe to ignore, because we know the number of tokens
                    }

                    if (positionIncrement > 0 && multiTerms.size() > 0) {
                        if (enablePositionIncrements) {
                            mpq.add(multiTerms.toArray(new Term[multiTerms.size()]), position);
                        } else {
                            mpq.add(multiTerms.toArray(new Term[multiTerms.size()]));
                        }
                        multiTerms.clear();
                    }
                    position += positionIncrement;
                    //LUCENE 4 UPGRADE instead of string term we can convert directly from utf-16 to utf-8 
                    multiTerms.add(new Term(field, termToByteRef(termAtt, new BytesRef())));
                }
                if (enablePositionIncrements) {
                    mpq.add(multiTerms.toArray(new Term[multiTerms.size()]), position);
                } else {
                    mpq.add(multiTerms.toArray(new Term[multiTerms.size()]));
                }
                return wrapSmartNameQuery(mpq, smartNameFieldMappers, parseContext);
            } else {
                PhraseQuery pq = new PhraseQuery();
                pq.setSlop(phraseSlop);
                int position = -1;


                for (int i = 0; i < numTokens; i++) {
                    int positionIncrement = 1;

                    try {
                        boolean hasNext = buffer.incrementToken();
                        assert hasNext == true;
                        if (posIncrAtt != null) {
                            positionIncrement = posIncrAtt.getPositionIncrement();
                        }
                    } catch (IOException e) {
                        // safe to ignore, because we know the number of tokens
                    }

                    if (enablePositionIncrements) {
                        position += positionIncrement;
                        //LUCENE 4 UPGRADE instead of string term we can convert directly from utf-16 to utf-8
                        pq.add(new Term(field, termToByteRef(termAtt, new BytesRef())), position);
                    } else {
                        pq.add(new Term(field, termToByteRef(termAtt, new BytesRef())));
                    }
                }
                return wrapSmartNameQuery(pq, smartNameFieldMappers, parseContext);
            }
        } else if (type == Type.PHRASE_PREFIX) {
            MultiPhrasePrefixQuery mpq = new MultiPhrasePrefixQuery();
            mpq.setSlop(phraseSlop);
            mpq.setMaxExpansions(maxExpansions);
            List<Term> multiTerms = new ArrayList<Term>();
            int position = -1;
            for (int i = 0; i < numTokens; i++) {
                int positionIncrement = 1;
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                    if (posIncrAtt != null) {
                        positionIncrement = posIncrAtt.getPositionIncrement();
                    }
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }

                if (positionIncrement > 0 && multiTerms.size() > 0) {
                    if (enablePositionIncrements) {
                        mpq.add(multiTerms.toArray(new Term[multiTerms.size()]), position);
                    } else {
                        mpq.add(multiTerms.toArray(new Term[multiTerms.size()]));
                    }
                    multiTerms.clear();
                }
                position += positionIncrement;
                //LUCENE 4 UPGRADE instead of string term we can convert directly from utf-16 to utf-8
                multiTerms.add(new Term(field, termToByteRef(termAtt, new BytesRef())));
            }
            if (enablePositionIncrements) {
                mpq.add(multiTerms.toArray(new Term[multiTerms.size()]), position);
            } else {
                mpq.add(multiTerms.toArray(new Term[multiTerms.size()]));
            }
            return wrapSmartNameQuery(mpq, smartNameFieldMappers, parseContext);
        }

        throw new ElasticSearchIllegalStateException("No type found for [" + type + "]");
    }

    private Query newTermQuery(@Nullable FieldMapper mapper, Term term) {
        if (maxEdits != null) {
            if (mapper != null) {
                Query query = mapper.fuzzyQuery(term.text(), maxEdits, prefixLength, maxExpansions, transpositions);
                if (query instanceof FuzzyQuery) {
                    QueryParsers.setRewriteMethod((FuzzyQuery) query, fuzzyRewriteMethod);
                }
            }
            String text = term.text();
            //LUCENE 4 UPGRADE we need to document that this should now be an int rather than a float
            int edits = FuzzyQuery.floatToEdits(Float.parseFloat(maxEdits),
                    text.codePointCount(0, text.length()));
            FuzzyQuery query = new FuzzyQuery(term, edits, prefixLength, maxExpansions, transpositions);
            QueryParsers.setRewriteMethod(query, rewriteMethod);
            return query;
        }
        if (mapper != null) {
            Query termQuery = mapper.queryStringTermQuery(term);
            if (termQuery != null) {
                return termQuery;
            }
        }
        return new TermQuery(term);
    }

    private static BytesRef termToByteRef(CharTermAttribute attr, BytesRef ref) {
        UnicodeUtil.UTF16toUTF8WithHash(attr.buffer(), 0, attr.length(), ref);
        return ref;
    }

    protected Query zeroTermsQuery() {
        return zeroTermsQuery == ZeroTermsQuery.NONE ? MatchNoDocsQuery.INSTANCE : Queries.MATCH_ALL_QUERY;
    }
}