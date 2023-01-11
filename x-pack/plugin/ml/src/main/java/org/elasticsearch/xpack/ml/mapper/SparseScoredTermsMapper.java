/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class SparseScoredTermsMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "sparse_scored_terms";
    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new SparseScoredTermsMapper.Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings()))
    );

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            FIELD_TYPE.freeze();
        }
        public static TextSearchInfo TEXT_SEARCH_INFO = new TextSearchInfo(
            FIELD_TYPE,
            null,
            Lucene.KEYWORD_ANALYZER,
            Lucene.KEYWORD_ANALYZER
        );
    }

    public SparseScoredTermsMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
    }

    private record SparseTermsAndScores(List<String> terms, List<Integer> scores) {

    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SparseTermsAndScores, Void> SPARSE_TERMS_MAPPED_FIELD_PARSER =
        new ConstructingObjectParser<>(
            "sparse_terms_mapped_field",
            false,
            a -> new SparseTermsAndScores((List<String>) a[0], (List<Integer>) a[1])
        );
    static {
        SPARSE_TERMS_MAPPED_FIELD_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), new ParseField("terms"));
        SPARSE_TERMS_MAPPED_FIELD_PARSER.declareIntArray(ConstructingObjectParser.constructorArg(), new ParseField("scores"));
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        context.path().add(simpleName());
        XContentParser.Token token;
        XContentSubParser subParser = null;
        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            // should be an object
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            subParser = new XContentSubParser(context.parser());
            SparseTermsAndScores doc = SPARSE_TERMS_MAPPED_FIELD_PARSER.apply(subParser, null);
            if (doc.terms.size() != doc.scores.size()) {
                throw new MapperParsingException(
                    Strings.format(
                        "failed to parse field [%s] of type [%s], [terms] and [scores] must have equal length; found [%d] and [%d]",
                        fieldType().name(),
                        CONTENT_TYPE,
                        doc.terms.size(),
                        doc.scores.size()
                    )
                );
            }
            int i = 0;
            for (Integer score : doc.scores) {
                if (score < 0) {
                    throw new MapperParsingException(
                        Strings.format(
                            "failed to parse field [%s] of type [%s], [scores] must be non-negative; found [%d] at position [%d]",
                            fieldType().name(),
                            CONTENT_TYPE,
                            score,
                            i
                        )
                    );
                }
                ++i;
            }
            if (doc.terms.size() > Sets.newHashSet(doc.terms).size()) {
                throw new MapperParsingException(
                    Strings.format(
                        "failed to parse field [%s] of type [%s]; requires unique items in [terms]",
                        fieldType().name(),
                        CONTENT_TYPE
                    )
                );
            }
            context.doc()
                .addWithKey(fieldType().name(), new Field(name(), new TermCountStream(doc.terms, doc.scores), Defaults.FIELD_TYPE));
            context.addToFieldNames(fieldType().name());
        } catch (Exception ex) {
            if (ignoreMalformed.value() == false) {
                if (ex instanceof MapperParsingException) {
                    throw ex;
                }
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", ex, fieldType().name(), fieldType().typeName());
            }

            if (subParser != null) {
                // close the subParser so we advance to the end of the object
                subParser.close();
            }
            context.addIgnoredField(fieldType().name());
        }
        context.path().remove();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), ignoreMalformedByDefault).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    private static class TermCountStream extends TokenStream {

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
        private final TermFrequencyAttribute termFreq = addAttribute(TermFrequencyAttribute.class);

        int pos = 0;
        private final List<String> term;
        private final List<Integer> count;

        private TermCountStream(List<String> term, List<Integer> count) {
            this.term = term;
            this.count = count;
        }

        @Override
        public void end() throws IOException {
            super.end();
            pos = term.size();
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            pos = 0;
        }

        @Override
        public boolean incrementToken() {
            if (pos < term.size()) {
                while (pos < term.size() && count.get(pos) <= 0) {
                    pos++;
                }
                if (pos >= term.size()) {
                    return false;
                }
                termAtt.setEmpty().append(term.get(pos));
                offsetAtt.setOffset(pos, pos + 1);
                termFreq.setTermFrequency(count.get(pos));
                pos++;
                return true;
            }
            return false;
        }
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Explicit<Boolean>> ignoreMalformed;

        /**
         * Creates a new Builder with a field name
         *
         * @param name the field name
         */
        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> ((SparseScoredTermsMapper) m).ignoreMalformed,
                ignoreMalformedByDefault
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { ignoreMalformed, meta };
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            return new SparseScoredTermsMapper(
                name,
                new SparseScoredTermsFieldType(context.buildFullName(name), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final class SparseScoredTermsFieldType extends TermBasedFieldType {

        // TODO we may want regular term searches against this field as well. Treating the terms as keywords
        public SparseScoredTermsFieldType(String name, Map<String, String> meta) {
            super(name, true, false, false, Defaults.TEXT_SEARCH_INFO, meta);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

}
