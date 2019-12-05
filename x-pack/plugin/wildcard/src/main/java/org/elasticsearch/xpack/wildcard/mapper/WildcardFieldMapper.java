/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A {@link FieldMapper} for indexing a keyword fields with ngrams for efficient wildcard matching
 */
public class WildcardFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "wildcard_keyword";
    public static short MAX_NUM_CHARS_COUNT = 6; //maximum allowed number of characters per ngram

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new WildcardFieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStoreTermVectorOffsets(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
        public static final String MATCH_TYPE_DOC_VALUES = "doc_values";
        public static final String MATCH_TYPE_BINARY_DOC_VALUES = "binary_doc_values";
        public static final String MATCH_TYPE_POSITION = "positions";
        
    }

    public static class Builder extends FieldMapper.Builder<Builder, WildcardFieldMapper> {
        private int numChars = 3;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        protected String matchType = Defaults.MATCH_TYPE_BINARY_DOC_VALUES;
        

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if(docValues == false) {
                throw new MapperParsingException("The field [" + name +
                        "] cannot have doc values = false");                
            }
            return this;
        }
        
        @Override
        public Builder index(boolean index) {
            if(index == false) {
                throw new MapperParsingException("The field [" + name +
                        "] cannot have index = false");                
            }
            return this;
        }

        public Builder numChars(int numChars) {
            if ((numChars > MAX_NUM_CHARS_COUNT) || (numChars < 1)) {
                throw new MapperParsingException("The number of characters for ngrams in field [" + name +
                    "] should be in the range [1, " + MAX_NUM_CHARS_COUNT + "]");
            }
            this.numChars = numChars;
            return this;
        }
        
        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }        
        
        public Builder matchType(String matchType) {
            if (Defaults.MATCH_TYPE_DOC_VALUES.equals(matchType) == false && 
                   Defaults.MATCH_TYPE_BINARY_DOC_VALUES.equals(matchType) == false && 
                   Defaults.MATCH_TYPE_POSITION.equals(matchType) == false) {
                throw new IllegalArgumentException("[match_type] must be " + Defaults.MATCH_TYPE_DOC_VALUES + " or "
                        + Defaults.MATCH_TYPE_BINARY_DOC_VALUES + " or "
                        + Defaults.MATCH_TYPE_POSITION + ", got " + matchType);
            }
            this.matchType = matchType;
            return this;
        }   
        
        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setNumChars(numChars);
            fieldType().setMatchType(matchType);
            if (matchType.equals(Defaults.MATCH_TYPE_POSITION)) { 
                fieldType().setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            } else {
                fieldType().setIndexOptions(IndexOptions.DOCS);                
            }
        }

        @Override
        public WildcardFieldType fieldType() {
            return (WildcardFieldType) super.fieldType();
        }

        @Override
        public WildcardFieldMapper build(BuilderContext context) {
            setupFieldType(context);            
            return new WildcardFieldMapper(
                    name, fieldType, defaultFieldType, ignoreAbove, matchType, numChars,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
        
        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            // Suspected parse sequencing problem here - if match_type not set yet we don't know
            // if this is appropriate or not....
//            if (matchType.equals(Defaults.MATCH_TYPE_DOC_VALUES)) {
//                if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
//                    throw new IllegalArgumentException("The [wildcard] field does not support positions with match_type "
//                            + Defaults.MATCH_TYPE_DOC_VALUES+", got [index_options]="
//                            + indexOptionToString(indexOptions));
//                }
//            }
            return super.indexOptions(indexOptions);
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
                if (propName.equals("num_chars")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [numChars] cannot be null.");
                    }
                    builder.numChars(XContentMapValues.nodeIntegerValue(propNode));
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("match_type")) {
                    builder.matchType(XContentMapValues.nodeStringValue(propNode, Defaults.MATCH_TYPE_BINARY_DOC_VALUES));
                    iterator.remove();
                }
            }            
            
            return builder;
        }
    }
    
     public static final char TOKEN_START_OR_END_CHAR = 0;
    // A visible character to aid debug
//     public static final char TOKEN_START_OR_END_CHAR = '$';
    
    
     public static final class WildcardFieldType extends MappedFieldType {
        private int numChars;
        private String matchType;

        public WildcardFieldType() {            
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);            
        }

        protected WildcardFieldType(WildcardFieldType ref) {
            super(ref);
        }

        public WildcardFieldType clone() {
            return new WildcardFieldType(this);
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
            
            
            if (matchType.equals(Defaults.MATCH_TYPE_POSITION)) {
                return new WildcardPositionBasedQuery(name(), patternStructure, numChars);
            }

            
            BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
            
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
                    fragment = fragment + TOKEN_START_OR_END_CHAR;
                }
                
                if (fragment.length() == numChars) {
                    TermQuery tq = new TermQuery(new Term(name(), fragment));
                    bqBuilder.add(new BooleanClause(tq, Occur.MUST));                    
                } else if (fragment.length() > numChars) {
                    // Break fragment into multiple Ngrams                
                    KeywordTokenizer kt = new KeywordTokenizer(256);
                    kt.setReader(new StringReader(fragment));
                    TokenFilter filter = new NGramTokenFilter(kt, numChars, numChars, false);
                    CharTermAttribute termAtt = filter.addAttribute(CharTermAttribute.class);
                    String lastUnusedToken = null;
                    try {
                        filter.reset();
                        int nextRequiredCoverage = 0;
                        int charPos = 0;
                        // minimise number of terms searched - eg for "1234567" and 4grams we only need terms 
                        // `1234` and `4567` - no need to search for 2345 and 3456
                        while (filter.incrementToken()) {                            
                            if (charPos == nextRequiredCoverage) {
                                TermQuery tq = new TermQuery(new Term(name(), termAtt.toString()));
                                bqBuilder.add(new BooleanClause(tq, Occur.MUST));
                                nextRequiredCoverage = charPos + termAtt.length() - 1;
                            } else {
                                lastUnusedToken = termAtt.toString();
                            }
                            charPos++;
                        }
                        if (lastUnusedToken != null) {
                            // given `cake` and 3 grams the loop above would output only `cak` and we need to add trailing
                            // `ake` to complete the logic.
                            TermQuery tq = new TermQuery(new Term(name(), lastUnusedToken));
                            bqBuilder.add(new BooleanClause(tq, Occur.MUST));                            
                        }
                        kt.end();
                        kt.close();
                    } catch(IOException ioe) {
                        throw new ElasticsearchParseException("Error parsing wildcard query pattern fragment ["+fragment+"]");
                    }
                } else {
                    // fragment is smaller than smallest ngram size
                    if (patternStructure.openEnd || i < patternStructure.fragments.length - 1) {
                        // fragment occurs mid-string so will need a wildcard query
                        WildcardQuery wq = new WildcardQuery(new Term(name(),fragment+"*"));
                        wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
                        bqBuilder.add(new BooleanClause(wq, Occur.MUST));
                    } else {
                        // fragment occurs at end of string so can rely on Jim's indexing rule to optimise 
                        // *foo by indexing smaller ngrams at the end of a string
                        TermQuery tq = new TermQuery(new Term(name(), fragment));
                        bqBuilder.add(new BooleanClause(tq, Occur.MUST));
                    }
                }
            }
            
            BooleanQuery approximation = bqBuilder.build();
            if (patternStructure.isMatchAll()) {
                return new MatchAllDocsQuery();
            } 
            if (approximation.clauses().size() > 1 || patternStructure.needsVerification()) {
                BooleanQuery.Builder verifyingBuilder = new BooleanQuery.Builder();
                verifyingBuilder.add(new BooleanClause(approximation, Occur.MUST));
                if (matchType.equals(Defaults.MATCH_TYPE_DOC_VALUES)) {                
                    verifyingBuilder.add(new BooleanClause(new WildcardOnDvQuery(name(), wildcardPattern), Occur.MUST));
                } else {
                    assert matchType.equals(Defaults.MATCH_TYPE_BINARY_DOC_VALUES);
                    verifyingBuilder.add(new BooleanClause(new WildcardOnBinaryDvQuery(name(), wildcardPattern), Occur.MUST));
                }
                return verifyingBuilder.build();
            }
            return approximation;
        }                

        int numChars() {
            return numChars;
        }

        void setNumChars(int numChars) {
            this.numChars = numChars;
        }

        void setMatchType(String matchType) {
            this.matchType = matchType;
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
        public Query termsQuery(List<?> values, QueryShardContext context) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            for (Object value : values) {
                bq.add(termQuery(value, context), Occur.SHOULD);
            }
            return new ConstantScoreQuery(bq.build());
        }                
    }

    private int ignoreAbove;
    private int numChars;
    private String matchType;

    private WildcardFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                int ignoreAbove, String matchType, int numChars, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreAbove = ignoreAbove;
        this.matchType = matchType;
        this.numChars = numChars;
        if (matchType.equals(Defaults.MATCH_TYPE_POSITION)) {
            assert fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        
        } else {
            assert fieldType.indexOptions() == IndexOptions.DOCS;
            
        }
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
        builder.field("num_chars", fieldType().numChars());
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
        if (includeDefaults || matchType != Defaults.MATCH_TYPE_BINARY_DOC_VALUES) {
            builder.field("match_type", matchType);
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

        if (value == null || value.length() > ignoreAbove) {
            return;
        }
        
        createFields(value, fields);        
    }   
    
    void createFields(String value, List<IndexableField>fields) {
        KeywordTokenizer kt = new KeywordTokenizer(256);
        kt.setReader(new StringReader(TOKEN_START_OR_END_CHAR+ value +TOKEN_START_OR_END_CHAR));
        TokenFilter filter = new TaperedNgramTokenFilter(kt, fieldType().numChars);
        
        Field field = new Field(fieldType().name(), filter, fieldType());
        fields.add(field);
        
        if (matchType.equals(Defaults.MATCH_TYPE_DOC_VALUES)) {        
            Field dvField = new SortedSetDocValuesField(fieldType().name(), new BytesRef(value));        
            fields.add(dvField);
        }
        if (matchType.equals(Defaults.MATCH_TYPE_BINARY_DOC_VALUES)) {
            Field dvField = new BinaryDocValuesField(fieldType().name(), new BytesRef(value));        
            fields.add(dvField);            
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
        this.matchType = ((WildcardFieldMapper) mergeWith).matchType;
        this.numChars = ((WildcardFieldMapper) mergeWith).numChars;
    }    
}
