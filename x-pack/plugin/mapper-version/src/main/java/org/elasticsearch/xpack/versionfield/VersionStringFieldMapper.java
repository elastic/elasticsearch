/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.versionfield.VersionEncoder.EncodedVersion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.xpack.versionfield.VersionEncoder.encodeVersion;

/**
 * A {@link FieldMapper} for indexing fields with version strings.
 */
public class VersionStringFieldMapper extends FieldMapper {

    private static final byte[] MIN_VALUE = new byte[16];
    private static final byte[] MAX_VALUE = new byte[16];
    static {
        Arrays.fill(MIN_VALUE, (byte) 0);
        Arrays.fill(MAX_VALUE, (byte) -1);
    }

    public static final String CONTENT_TYPE = "version";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        Builder(String name) {
            super(name);
        }

        private VersionStringFieldType buildFieldType(MapperBuilderContext context, FieldType fieldtype) {
            return new VersionStringFieldType(context.buildFullName(name), fieldtype, meta.getValue());
        }

        @Override
        public VersionStringFieldMapper build(MapperBuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            return new VersionStringFieldMapper(
                name,
                fieldtype,
                buildFieldType(context, fieldtype),
                multiFieldsBuilder.build(this, context),
                copyTo.build()
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class VersionStringFieldType extends TermBasedFieldType {

        private VersionStringFieldType(String name, FieldType fieldType, Map<String, String> meta) {
            super(name, true, false, true, new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER), meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            return wildcardQuery(value + "*", method, caseInsensitive, context);
        }

        /**
         * We cannot simply use RegexpQuery directly since we use the encoded terms from the dictionary, but the
         * automaton in the query will assume unencoded terms. We are running through all terms, decode them and
         * then run them through the automaton manually instead. This is not as efficient as intersecting the original
         * Terms with the compiled automaton, but we expect the number of distinct version terms indexed into this field
         * to be low enough and the use of "regexp" queries on this field rare enough to brute-force this
         */
        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            @Nullable MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context
        ) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                );
            }
            RegexpQuery query = new RegexpQuery(new Term(name(), new BytesRef(value)), syntaxFlags, matchFlags, maxDeterminizedStates) {

                @Override
                protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
                    return new FilteredTermsEnum(terms.iterator(), false) {

                        @Override
                        protected AcceptStatus accept(BytesRef term) throws IOException {
                            byte[] decoded = VersionEncoder.decodeVersion(term).getBytes(StandardCharsets.UTF_8);
                            boolean accepted = compiled.runAutomaton.run(decoded, 0, decoded.length);
                            if (accepted) {
                                return AcceptStatus.YES;
                            }
                            return AcceptStatus.NO;
                        }
                    };
                }
            };

            if (method != null) {
                query.setRewriteMethod(method);
            }
            return query;
        }

        /**
         * We cannot simply use FuzzyQuery directly since we use the encoded terms from the dictionary, but the
         * automaton in the query will assume unencoded terms. We are running through all terms, decode them and
         * then run them through the automaton manually instead. This is not as efficient as intersecting the original
         * Terms with the compiled automaton, but we expect the number of distinct version terms indexed into this field
         * to be low enough and the use of "fuzzy" queries on this field rare enough to brute-force this
         */
        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[fuzzy] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                );
            }
            return new FuzzyQuery(
                new Term(name(), (BytesRef) value),
                fuzziness.asDistance(BytesRefs.toString(value)),
                prefixLength,
                maxExpansions,
                transpositions
            ) {
                @Override
                protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
                    ByteRunAutomaton runAutomaton = getAutomata().runAutomaton;

                    return new FilteredTermsEnum(terms.iterator(), false) {

                        @Override
                        protected AcceptStatus accept(BytesRef term) throws IOException {
                            byte[] decoded = VersionEncoder.decodeVersion(term).getBytes(StandardCharsets.UTF_8);
                            boolean accepted = runAutomaton.run(decoded, 0, decoded.length);
                            if (accepted) {
                                return AcceptStatus.YES;
                            }
                            return AcceptStatus.NO;
                        }
                    };
                }
            };
        }

        @Override
        public Query wildcardQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "[wildcard] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                );
            }

            VersionFieldWildcardQuery query = new VersionFieldWildcardQuery(new Term(name(), value), caseInsensitive);
            QueryParsers.setRewriteMethod(query, method);
            return query;
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            String valueAsString;
            if (value instanceof String) {
                valueAsString = (String) value;
            } else if (value instanceof BytesRef) {
                // encoded string, need to re-encode
                valueAsString = ((BytesRef) value).utf8ToString();
            } else {
                throw new IllegalArgumentException("Illegal value type: " + value.getClass() + ", value: " + value);
            }
            return encodeVersion(valueAsString).bytesRef;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, VersionStringDocValuesField::new);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return VERSION_DOCVALUE.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            checkNoFormat(format);
            checkNoTimeZone(timeZone);
            return VERSION_DOCVALUE;
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            BytesRef lower = lowerTerm == null ? null : indexedValueForSearch(lowerTerm);
            BytesRef upper = upperTerm == null ? null : indexedValueForSearch(upperTerm);
            return new TermRangeQuery(name(), lower, upper, includeLower, includeUpper);
        }
    }

    private final FieldType fieldType;

    private VersionStringFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.fieldType = fieldType;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public VersionStringFieldType fieldType() {
        return (VersionStringFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        String versionString;
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        } else {
            versionString = parser.textOrNull();
        }

        if (versionString == null) {
            return;
        }

        EncodedVersion encoding = encodeVersion(versionString);
        BytesRef encodedVersion = encoding.bytesRef;
        context.doc().add(new Field(fieldType().name(), encodedVersion, fieldType));
        context.doc().add(new SortedSetDocValuesField(fieldType().name(), encodedVersion));
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
        return concat;
    }

    public static DocValueFormat VERSION_DOCVALUE = new DocValueFormat() {

        @Override
        public String getWriteableName() {
            return "version";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String format(BytesRef value) {
            return VersionEncoder.decodeVersion(value);
        }

        @Override
        public BytesRef parseBytesRef(Object value) {
            return VersionEncoder.encodeVersion(value.toString()).bytesRef;
        }

        @Override
        public String toString() {
            return getWriteableName();
        }
    };

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
