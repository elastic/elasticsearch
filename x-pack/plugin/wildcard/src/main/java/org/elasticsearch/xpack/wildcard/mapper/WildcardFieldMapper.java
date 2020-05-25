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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

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
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;


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
                if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                }
            }

            return builder;
        }
    }

     public static final char TOKEN_START_OR_END_CHAR = 0;

     public static final class WildcardFieldType extends MappedFieldType {

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

        // Holds parsed information about the wildcard pattern
        static class PatternStructure {
            boolean openStart, openEnd, hasSymbols;
            int lastGap =0;
            int wildcardCharCount, wildcardStringCount;
            String[] fragments;
            Integer []  precedingGapSizes;
            final String pattern;

            @SuppressWarnings("fallthrough") // Intentionally uses fallthrough mirroring implementation in Lucene's WildcardQuery
            PatternStructure (String wildcardText) {
                this.pattern = wildcardText;
                ArrayList<String> fragmentList = new ArrayList<>();
                ArrayList<Integer> precedingGapSizeList = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < wildcardText.length();) {
                    final int c = wildcardText.codePointAt(i);
                    int length = Character.charCount(c);
                    switch (c) {
                    case WildcardQuery.WILDCARD_STRING:
                        if (i == 0) {
                            openStart = true;
                        }
                        openEnd = true;
                        hasSymbols = true;
                        wildcardStringCount++;

                        if (sb.length() > 0) {
                            precedingGapSizeList.add(lastGap);
                            fragmentList.add(sb.toString());
                            sb = new StringBuilder();
                        }
                        lastGap = Integer.MAX_VALUE;
                        break;
                    case WildcardQuery.WILDCARD_CHAR:
                        if (i == 0) {
                            openStart = true;
                        }
                        hasSymbols = true;
                        wildcardCharCount++;
                        openEnd = true;
                        if (sb.length() > 0) {
                            precedingGapSizeList.add(lastGap);
                            fragmentList.add(sb.toString());
                            sb = new StringBuilder();
                            lastGap = 0;
                        }

                        if (lastGap != Integer.MAX_VALUE) {
                            lastGap++;
                        }
                        break;
                    case WildcardQuery.WILDCARD_ESCAPE:
                        // add the next codepoint instead, if it exists
                        if (i + length < wildcardText.length()) {
                            final int nextChar = wildcardText.codePointAt(i + length);
                            length += Character.charCount(nextChar);
                            sb.append(Character.toChars(nextChar));
                            openEnd = false;
                            break;
                        } // else fallthru, lenient parsing with a trailing \
                    default:
                        openEnd = false;
                        sb.append(Character.toChars(c));
                    }
                    i += length;
                }
                if (sb.length() > 0) {
                    precedingGapSizeList.add(lastGap);
                    fragmentList.add(sb.toString());
                    lastGap = 0;
                }
                fragments = fragmentList.toArray(new String[0]);
                precedingGapSizes = precedingGapSizeList.toArray(new Integer[0]);

            }

            public boolean needsVerification() {
                // Return true if term queries are not enough evidence
                if (fragments.length == 1 && wildcardCharCount == 0) {
                    // The one case where we don't need verification is when
                    // we have a single fragment and no ? characters
                    return false;
                }
                return true;
            }

            // Returns number of positions for last gap (Integer.MAX means unlimited gap)
            public int getPrecedingGapSize(int fragmentNum) {
                return precedingGapSizes[fragmentNum];
            }

            public boolean isMatchAll() {
                return fragments.length == 0 && wildcardStringCount >0 && wildcardCharCount ==0;
            }

            @Override
            public int hashCode() {
                return pattern.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                PatternStructure other = (PatternStructure) obj;
                return pattern.equals(other.pattern);
            }


        }


        @Override
        public Query wildcardQuery(String wildcardPattern, RewriteMethod method, QueryShardContext context) {
            PatternStructure patternStructure = new PatternStructure(wildcardPattern);
            ArrayList<String> tokens = new ArrayList<>();

            for (int i = 0; i < patternStructure.fragments.length; i++) {
                String fragment = patternStructure.fragments[i];
                int fLength = fragment.length();
                if (fLength == 0) {
                    continue;
                }

                // Add any start/end of string character
                if (i == 0 && patternStructure.openStart == false) {
                    // Start-of-string anchored (is not a leading wildcard)
                    fragment = TOKEN_START_OR_END_CHAR + fragment;
                }
                if (patternStructure.openEnd == false && i == patternStructure.fragments.length - 1) {
                    // End-of-string anchored (is not a trailing wildcard)
                    fragment = fragment + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;
                }
                if (fragment.codePointCount(0, fragment.length()) <= NGRAM_SIZE) {
                    tokens.add(fragment);
                } else {
                    // Break fragment into multiple Ngrams
                    TokenStream tokenizer = WILDCARD_ANALYZER.tokenStream(name(), fragment);
                    CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
                    String lastUnusedToken = null;
                    try {
                        tokenizer.reset();
                        boolean takeThis = true;
                        // minimise number of terms searched - eg for "12345" and 3grams we only need terms
                        // `123` and `345` - no need to search for 234. We take every other ngram.
                        while (tokenizer.incrementToken()) {
                            String tokenValue = termAtt.toString();
                            if (takeThis) {
                                tokens.add(tokenValue);
                            } else {
                                lastUnusedToken = tokenValue;
                            }
                            // alternate
                            takeThis = !takeThis;
                        }
                        if (lastUnusedToken != null) {
                            // given `cake` and 3 grams the loop above would output only `cak` and we need to add trailing
                            // `ake` to complete the logic.
                            tokens.add(lastUnusedToken);
                        }
                        tokenizer.end();
                        tokenizer.close();
                    } catch (IOException ioe) {
                        throw new ElasticsearchParseException("Error parsing wildcard query pattern fragment [" + fragment + "]");
                    }
                }
            }

            if (patternStructure.isMatchAll()) {
                return new MatchAllDocsQuery();
            }
            BooleanQuery approximation = createApproximationQuery(tokens);
            if (approximation.clauses().size() > 1 || patternStructure.needsVerification()) {
                BooleanQuery.Builder verifyingBuilder = new BooleanQuery.Builder();
                verifyingBuilder.add(new BooleanClause(approximation, Occur.MUST));
                Automaton automaton = WildcardQuery.toAutomaton(new Term(name(), wildcardPattern));
                verifyingBuilder.add(new BooleanClause(new AutomatonQueryOnBinaryDv(name(), wildcardPattern, automaton), Occur.MUST));
                return verifyingBuilder.build();
            }
            return approximation;
        }

        private BooleanQuery createApproximationQuery(ArrayList<String> tokens) {
            BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
            if (tokens.size() <= MAX_CLAUSES_IN_APPROXIMATION_QUERY) {
                for (String token : tokens) {
                    addClause(token, bqBuilder);
                }
                return bqBuilder.build();
            }
            // Thin out the number of clauses using a selection spread evenly across the range
            float step = (float) (tokens.size() - 1) / (float) (MAX_CLAUSES_IN_APPROXIMATION_QUERY - 1); // set step size
            for (int i = 0; i < MAX_CLAUSES_IN_APPROXIMATION_QUERY; i++) {
                addClause(tokens.get(Math.round(step * i)), bqBuilder); // add each element of a position which is a multiple of step
            }
            // TODO we can be smarter about pruning here. e.g.
            // * Avoid wildcard queries if there are sufficient numbers of other terms that are full 3grams that are cheaper term queries
            // * We can select terms on their scarcity rather than even spreads across the search string.

            return bqBuilder.build();
        }

        private void addClause(String token, BooleanQuery.Builder bqBuilder) {
            assert token.codePointCount(0, token.length()) <= NGRAM_SIZE;
            if (token.codePointCount(0, token.length()) == NGRAM_SIZE) {
                TermQuery tq = new TermQuery(new Term(name(), token));
                bqBuilder.add(new BooleanClause(tq, Occur.MUST));
            } else {
                WildcardQuery wq = new WildcardQuery(new Term(name(), token + "*"));
                wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
                bqBuilder.add(new BooleanClause(wq, Occur.MUST));
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

    // For internal use by Lucene only - used to define ngram index
    final MappedFieldType ngramFieldType;

    void createFields(String value, Document parseDoc, List<IndexableField>fields) throws IOException {
        if (value == null || value.length() > ignoreAbove) {
            return;
        }
        String ngramValue = TOKEN_START_OR_END_CHAR + value + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;
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

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        this.ignoreAbove = ((WildcardFieldMapper) other).ignoreAbove;
    }
}
