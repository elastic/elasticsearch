/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
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
import org.elasticsearch.xpack.wildcard.mapper.regex.AutomatonTooComplexException;
import org.elasticsearch.xpack.wildcard.mapper.regex.Expression;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    }

    public static class Builder extends FieldMapper.Builder<Builder, WildcardFieldMapper> {
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
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
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

        // Currently these settings are not tweakable by end users, however, they are pretty advanced settings so may never be.
        private NGramApproximationSettings settings = new NGramApproximationSettings();

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
            
            Automaton dvAutomaton = WildcardQuery.toAutomaton(new Term(name(), wildcardPattern));
            
            //Unlike the doc values, ngram index has extra string-start and end characters. 
            String ngramWildcardPattern = TOKEN_START_OR_END_CHAR + wildcardPattern 
                    + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;            
            Automaton ngramAutomaton = WildcardQuery.toAutomaton(new Term(name(), ngramWildcardPattern));
            return automatonToQuery(wildcardPattern, wildcardPattern, ngramAutomaton, dvAutomaton);
        }
        
        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates, RewriteMethod method, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException("[regexp] queries cannot be executed when '" +
                        ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
            }
            failIfNotIndexed();
            //Unlike the doc values, ngram index has extra string-start and end characters. 
            String ngramPattern = TOKEN_START_OR_END_CHAR + value 
                    + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;            
            RegExp regex =new RegExp(value, flags);
            try {
                // TODO always lower case the ngram index and value? Could help with 
                // a) speed (less ngram variations to explore on disk) and 
                // b) use less disk space
                String openStart = TOKEN_START_OR_END_CHAR + ".*";
                if (ngramPattern.startsWith(openStart)) {
                    //".*" causes too many imagined beginnings in the Automaton to trace through.
                    //Rewrite to cut to the concrete path after .* to extract required ngrams
                    // TODO ideally we would trim the automaton paths rather than the regex string
                    ngramPattern = ngramPattern.substring(openStart.length());
                }
                Automaton automaton = regex.toAutomaton(maxDeterminizedStates);
                RegExp ngramRegex =new RegExp(ngramPattern, flags);
                Automaton ngramAutomaton = ngramRegex.toAutomaton(maxDeterminizedStates);
                return automatonToQuery(ngramPattern, value, ngramAutomaton, automaton);
            } catch (AutomatonTooComplexException e) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Regex /%s/ too complex for maxStatesTraced setting [%s].  Use a simpler regex or raise maxStatesTraced.", regex,
                        settings.getMaxStatesTraced()), e);
            }
            
        }
        

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                boolean transpositions, QueryShardContext context) {            
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException("[fuzzy] queries cannot be executed when '" +
                        ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
            }
            failIfNotIndexed();
            String searchTerm = BytesRefs.toString(value);
            FuzzyQuery fq = new FuzzyQuery(new Term(name(), searchTerm),
                    fuzziness.asDistance(searchTerm), prefixLength, maxExpansions, transpositions);

            
            String ngramFuzzyPattern = TOKEN_START_OR_END_CHAR + searchTerm + TOKEN_START_OR_END_CHAR + TOKEN_START_OR_END_CHAR;            
            
            FuzzyQuery ngramFq = new FuzzyQuery(new Term(name(), ngramFuzzyPattern),
                    fuzziness.asDistance(ngramFuzzyPattern), prefixLength + 1, maxExpansions, transpositions);

            // I'm assuming (perhaps incorrectly) that these are logically ORed...
            CompiledAutomaton[] ngramAutomata = ngramFq.getAutomata();
            CompiledAutomaton[] automata = fq.getAutomata();
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            assert ngramAutomata.length == automata.length;
            for (int i=0; i<automata.length;i++) {
                Query ngramQuery = automatonToQuery(ngramFuzzyPattern, searchTerm, ngramAutomata[i].automaton, automata[i].automaton);
                builder.add(ngramQuery, Occur.SHOULD);                                
            }
            return builder.build();
        }

        // The ngram and dv values can differ - null chars added to string beginning and ends in ngram index and used by
        // wildcard query expressions.
        protected Query automatonToQuery(String ngramValue, String dvValue, Automaton ngramAutomaton, Automaton dvAutomaton) {
            Query ngramApproximation;
            Expression<String> expression = new NGramExtractor(NGRAM_SIZE, settings.getMaxExpand(), settings.getMaxStatesTraced(),
                    settings.getMaxNgramsExtracted()).extract(ngramAutomaton).simplify();
            if (expression.alwaysTrue()) {
                if (settings.getRejectUnaccelerated()) {
                    throw new UnableToAccelerateRegexException(ngramValue, NGRAM_SIZE, name());
                }
                ngramApproximation = new MatchAllDocsQuery();
            } else if (expression.alwaysFalse()) {
                // Not sure what regex expression would cause this branch but cover it anyway.
                ngramApproximation = new MatchNoDocsQuery();
            } else {
                ngramApproximation = expression.transform(new ExpressionToFilterTransformer(name()));
            }
            //TODO - skip every other ngram in long strings
            //TODO - don't generate terms not in index.
            //TODO - prefer rare ngrams when pruning

            AutomatonQueryOnBinaryDv verifyingQuery = new AutomatonQueryOnBinaryDv(name(), dvValue, dvAutomaton);
            if (ngramApproximation != null && !(ngramApproximation instanceof MatchAllDocsQuery)) {
                // We can accelerate execution with the ngram query
                BooleanQuery.Builder verifyingBuilder = new BooleanQuery.Builder();
                verifyingBuilder.add(new BooleanClause(ngramApproximation, Occur.MUST));
                verifyingBuilder.add(new BooleanClause(verifyingQuery, Occur.MUST));
                return verifyingBuilder.build();
            }
            return verifyingQuery;
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
                    return new WildcardBytesBinaryDVIndexFieldData(indexSettings.getIndex(), fieldType.name());
                }};
        }

         @Override
         public ValuesSourceType getValuesSourceType() {
             return CoreValuesSourceType.BYTES;
         }

    }

    static class  WildcardBytesBinaryDVIndexFieldData extends BytesBinaryDVIndexFieldData{

        WildcardBytesBinaryDVIndexFieldData(Index index, String fieldName) {
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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
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

        createFields(value, parseDoc, fields);
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
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        this.ignoreAbove = ((WildcardFieldMapper) mergeWith).ignoreAbove;
    }
}
