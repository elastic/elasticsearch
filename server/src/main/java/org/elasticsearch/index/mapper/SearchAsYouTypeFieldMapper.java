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
package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FreqIsScoreSimilarity;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.nodeIndexOptionValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseMultiField;

public class SearchAsYouTypeFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "search_as_you_type";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new SearchAsYouTypeFieldType();
        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name,
                                            Map<String, Object> node,
                                            ParserContext parserContext) throws MapperParsingException {
            SearchAsYouTypeFieldMapper.Builder builder = new SearchAsYouTypeFieldMapper.Builder(name);
            NamedAnalyzer indexAnalyzer = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if ("type".equals(fieldName)) {
                    continue;
                } else if ("analyzer".equals(fieldName)) {
                    indexAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if ("index_options".equals(fieldName)) {
                    builder.indexOptions(nodeIndexOptionValue(fieldNode));
                    iterator.remove();
                } else if ("fields".equals(fieldName)) {
                    parseMultiField(builder, name, parserContext, fieldName, fieldNode)
                    iterator.remove();
                }
            }
            if (indexAnalyzer == null) {
                indexAnalyzer = parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer();
            }
            builder.indexAnalyzer(
                new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, new SearchAsYouTypeAnalyzer(indexAnalyzer.analyzer(), true))
            );
            builder.searchAnalyzer(
                new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, new SearchAsYouTypeAnalyzer(indexAnalyzer.analyzer(), false))
            );
            builder.similarity(new SimilarityProvider("freq_is_score", new FreqIsScoreSimilarity()));
            return builder;
        }

        private NamedAnalyzer getNamedAnalyzer(ParserContext parserContext, String name) {
            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(name);
            if (analyzer == null) {
                throw new IllegalArgumentException("Can't find default or mapped analyzer with name [" + name + "]");
            }
            return analyzer;
        }
    }

    private static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final boolean withEdgeNgram;

        SearchAsYouTypeAnalyzer(Analyzer delegate, boolean withEdgeNgram) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.withEdgeNgram = withEdgeNgram;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            // TODO we must find a way to add the last unigram term (michael jackson -> jackson)
            // TODO we should keep one weight per surface form otherwise the index writer sums the custom frequencies
            TokenStream filter = new FixedShingleFilter(components.getTokenStream(), 2);
            if (withEdgeNgram) {
                // TODO Should we make the [min|max]Gram configurable ?
                filter = new EdgeNGramTokenFilter(filter, 1, 20, true);
            }
            return new TokenStreamComponents(components.getTokenizer(), filter);
        }
    }

    private static final class SearchAsYouTypeFieldType extends TermBasedFieldType {
        SearchAsYouTypeFieldType(SearchAsYouTypeFieldType ref) {
            super(ref);
        }

        SearchAsYouTypeFieldType() {
            setTokenized(true);
        }

        @Override
        public MappedFieldType clone() {
            return new SearchAsYouTypeFieldType(this);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            // TODO: should we have a way to query for exact terms (no prefixes) ?
            return super.termQuery(value, context);
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            // we index prefixes
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    /**
     * Builder for {@link SearchAsYouTypeFieldMapper}
     */
    public static class Builder extends FieldMapper.Builder<Builder, SearchAsYouTypeFieldMapper> {
        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public SearchAsYouTypeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new SearchAsYouTypeFieldMapper(name, this.fieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public SearchAsYouTypeFieldMapper(String simpleName, MappedFieldType fieldType, Settings indexSettings,
                                      MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
    }

    @Override
    public SearchAsYouTypeFieldType fieldType() {
        return (SearchAsYouTypeFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // parse
        XContentParser parser = context.parser();
        Token token = parser.currentToken();

        List<TextAndWeight> inputs = new ArrayList<>();
        // ignore null values
        if (token == Token.VALUE_NULL) {
            return;
        } else if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                inputs.add(parse(context, token, parser));
            }
        } else {
            inputs.add(parse(context, token, parser));
        }

        for (TextAndWeight textAndWeight : inputs) {
            context.doc().add(
                new Field(name(), textAndWeight.text, fieldType) {
                    @Override
                    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
                        // TODO we should keep one weight per surface form otherwise the index writer sums the custom frequencies
                        TokenStream stream = super.tokenStream(analyzer, reuse);
                        if (textAndWeight.weight > 1) {
                            TermFrequencyAttribute att = stream.addAttribute(TermFrequencyAttribute.class);
                            att.setTermFrequency(textAndWeight.weight);
                        }
                        return stream;
                    }
                }
            );
        }
        List<IndexableField> fields = new ArrayList<>(1);
        createFieldNamesField(context, fields);
        for (IndexableField field : fields) {
            context.doc().add(field);
        }
        multiFields.parse(this, context);
    }

    private TextAndWeight parse(ParseContext parseContext, Token token, XContentParser parser) throws IOException {
        if (token == Token.VALUE_STRING) {
            String value = parser.textOrNull();
            return new TextAndWeight(value, 1);
        } else if (token == Token.START_OBJECT) {
            // TODO we should parse this object lazily
            Map<String, Object> inner = parser.map();
            String input = (String) inner.remove("input");
            Integer weight = (Integer) inner.remove("weight");
            return new TextAndWeight(input, weight == null ? 1 : weight);
        } else {
            throw new ParsingException(parser.getTokenLocation(), "failed to parse [" + parser.currentName()
                + "]: expected text or object, but got " + token.name());
        }
    }

    static class TextAndWeight {
        final String text;
        final int weight;

        public TextAndWeight(String text, int weight) {
            this.text = text;
            this.weight = weight;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName())
            .field("type", CONTENT_TYPE);
        if (fieldType().indexAnalyzer().name().equals("default") == false) {
            builder.field("analyzer", fieldType().indexAnalyzer().name());
        }
        multiFields.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // no-op
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
