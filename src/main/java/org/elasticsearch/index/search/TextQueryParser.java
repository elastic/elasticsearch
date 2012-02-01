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
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

public class TextQueryParser {

    public static enum Type {
        BOOLEAN,
        PHRASE,
        PHRASE_PREFIX
    }

    private final QueryParseContext parseContext;

    private final String fieldName;

    private final String text;

    private String analyzer;

    private BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    private boolean enablePositionIncrements = true;

    private int phraseSlop = 0;

    private String fuzziness = null;
    private int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
    private int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    public TextQueryParser(QueryParseContext parseContext, String fieldName, String text) {
        this.parseContext = parseContext;
        this.fieldName = fieldName;
        this.text = text;
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

    public void setFuzziness(String fuzziness) {
        this.fuzziness = fuzziness;
    }

    public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
    }

    public void setMaxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
    }

    public Query parse(Type type) {
        FieldMapper mapper = null;
        Term fieldTerm;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            fieldTerm = mapper.names().indexNameTerm();
        } else {
            fieldTerm = new Term(fieldName);
        }

        if (mapper != null && mapper.useFieldQueryWithQueryString()) {
            if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
                try {
                    return wrapSmartNameQuery(mapper.fieldQuery(text, parseContext), smartNameFieldMappers, parseContext);
                } finally {
                    QueryParseContext.setTypes(previousTypes);
                }
            } else {
                return wrapSmartNameQuery(mapper.fieldQuery(text, parseContext), smartNameFieldMappers, parseContext);
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

        TokenStream source;
        try {
            source = analyzer.reusableTokenStream(fieldTerm.field(), new FastStringReader(text));
            source.reset();
        } catch (IOException e) {
            source = analyzer.tokenStream(fieldTerm.field(), new FastStringReader(text));
        }
        CachingTokenFilter buffer = new CachingTokenFilter(source);
        CharTermAttribute termAtt = null;
        PositionIncrementAttribute posIncrAtt = null;
        int numTokens = 0;

        boolean success = false;
        try {
            buffer.reset();
            success = true;
        } catch (IOException e) {
            // success==false if we hit an exception
        }
        if (success) {
            if (buffer.hasAttribute(CharTermAttribute.class)) {
                termAtt = buffer.getAttribute(CharTermAttribute.class);
            }
            if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
                posIncrAtt = buffer.getAttribute(PositionIncrementAttribute.class);
            }
        }

        int positionCount = 0;
        boolean severalTokensAtSamePosition = false;

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
        try {
            // rewind the buffer stream
            buffer.reset();

            // close original stream - all tokens buffered
            source.close();
        } catch (IOException e) {
            // ignore
        }

        if (numTokens == 0) {
            return MatchNoDocsQuery.INSTANCE;
        } else if (type == Type.BOOLEAN) {
            if (numTokens == 1) {
                String term = null;
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                    term = termAtt.toString();
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }
                Query q = newTermQuery(mapper, fieldTerm.createTerm(term));
                return wrapSmartNameQuery(q, smartNameFieldMappers, parseContext);
            }
            BooleanQuery q = new BooleanQuery(positionCount == 1);
            for (int i = 0; i < numTokens; i++) {
                String term = null;
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                    term = termAtt.toString();
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }

                Query currentQuery = newTermQuery(mapper, fieldTerm.createTerm(term));
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
                    String term = null;
                    int positionIncrement = 1;
                    try {
                        boolean hasNext = buffer.incrementToken();
                        assert hasNext == true;
                        term = termAtt.toString();
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
                    multiTerms.add(fieldTerm.createTerm(term));
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
                    String term = null;
                    int positionIncrement = 1;

                    try {
                        boolean hasNext = buffer.incrementToken();
                        assert hasNext == true;
                        term = termAtt.toString();
                        if (posIncrAtt != null) {
                            positionIncrement = posIncrAtt.getPositionIncrement();
                        }
                    } catch (IOException e) {
                        // safe to ignore, because we know the number of tokens
                    }

                    if (enablePositionIncrements) {
                        position += positionIncrement;
                        pq.add(fieldTerm.createTerm(term), position);
                    } else {
                        pq.add(fieldTerm.createTerm(term));
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
                String term = null;
                int positionIncrement = 1;
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                    term = termAtt.toString();
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
                multiTerms.add(fieldTerm.createTerm(term));
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
        if (fuzziness != null) {
            if (mapper != null) {
                return mapper.fuzzyQuery(term.text(), fuzziness, fuzzyPrefixLength, maxExpansions);
            }
            return new FuzzyQuery(term, Float.parseFloat(fuzziness), fuzzyPrefixLength, maxExpansions);
        }
        if (mapper != null) {
            Query termQuery = mapper.queryStringTermQuery(term);
            if (termQuery != null) {
                return termQuery;
            }
        }
        return new TermQuery(term);
    }
}