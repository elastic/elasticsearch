/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordDocValuesField;
import org.elasticsearch.xpack.core.termsenum.action.SimpleTermCountEnum;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A {@link FieldMapper} that assigns every document the same value.
 */
public class ConstantKeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "constant_keyword";

    private static ConstantKeywordFieldMapper toType(FieldMapper in) {
        return (ConstantKeywordFieldMapper) in;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    public static class Builder extends FieldMapper.Builder {

        // This is defined as updateable because it can be updated once, from [null] to any value,
        // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
        private final Parameter<String> value = new Parameter<>("value", true, () -> null, (n, c, o) -> {
            if (o instanceof Number == false && o instanceof CharSequence == false) {
                throw new MapperParsingException("Property [value] on field [" + n + "] must be a number or a string, but got [" + o + "]");
            }
            return o.toString();
        }, m -> toType(m).fieldType().value, XContentBuilder::field, Objects::toString);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
            value.setSerializerCheck((id, ic, v) -> v != null);
            value.setMergeValidator((previous, current, c) -> previous == null || Objects.equals(previous, current));
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { value, meta };
        }

        @Override
        public ConstantKeywordFieldMapper build(MapperBuilderContext context) {
            return new ConstantKeywordFieldMapper(
                name,
                new ConstantKeywordFieldType(context.buildFullName(name), value.getValue(), meta.getValue())
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class ConstantKeywordFieldType extends ConstantFieldType {

        private final String value;

        public ConstantKeywordFieldType(String name, String value, Map<String, String> meta) {
            super(name, meta);
            this.value = value;
        }

        public ConstantKeywordFieldType(String name, String value) {
            this(name, value, Collections.emptyMap());
        }

        /** Return the value that this field wraps. This may be {@code null} if the field is not configured yet. */
        public String value() {
            return value;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return KeywordFieldMapper.CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new ConstantIndexFieldData.Builder(
                value,
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new ConstantKeywordDocValuesField(FieldData.toString(dv), n)
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return value == null ? ValueFetcher.EMPTY : ValueFetcher.singleton(value);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) {
            if (value == null) {
                return TermsEnum.EMPTY;
            }
            boolean matches = caseInsensitive
                ? value.toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT))
                : value.startsWith(prefix);
            if (matches == false) {
                return TermsEnum.EMPTY;
            }
            if (searchAfter != null) {
                if (searchAfter.compareTo(value) >= 0) {
                    // The constant value is before the searchAfter value so must be ignored
                    return TermsEnum.EMPTY;
                }
            }
            return new SimpleTermCountEnum(value);
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryRewriteContext context) {
            if (value == null) {
                return false;
            }
            return Regex.simpleMatch(pattern, value, caseInsensitive);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return value != null ? new MatchAllDocsQuery() : new MatchNoDocsQuery();
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            SearchExecutionContext context
        ) {
            if (this.value == null) {
                return new MatchNoDocsQuery();
            }

            final BytesRef valueAsBytesRef = new BytesRef(value);
            if (lowerTerm != null && BytesRefs.toBytesRef(lowerTerm).compareTo(valueAsBytesRef) >= (includeLower ? 1 : 0)) {
                return new MatchNoDocsQuery();
            }
            if (upperTerm != null && valueAsBytesRef.compareTo(BytesRefs.toBytesRef(upperTerm)) >= (includeUpper ? 1 : 0)) {
                return new MatchNoDocsQuery();
            }
            return new MatchAllDocsQuery();
        }

        @Override
        public Query fuzzyQuery(
            Object term,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context,
            @Nullable MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            if (this.value == null) {
                return new MatchNoDocsQuery();
            }

            final String termAsString = BytesRefs.toString(term);
            final int maxEdits = fuzziness.asDistance(termAsString);

            final int[] termText = new int[termAsString.codePointCount(0, termAsString.length())];
            for (int cp, i = 0, j = 0; i < termAsString.length(); i += Character.charCount(cp)) {
                termText[j++] = cp = termAsString.codePointAt(i);
            }
            final int termLength = termText.length;

            prefixLength = Math.min(prefixLength, termLength);
            final String suffix = UnicodeUtil.newString(termText, prefixLength, termText.length - prefixLength);
            final LevenshteinAutomata builder = new LevenshteinAutomata(suffix, transpositions);
            final String prefix = UnicodeUtil.newString(termText, 0, prefixLength);
            final Automaton automaton = builder.toAutomaton(maxEdits, prefix);

            final CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
            if (runAutomaton.run(this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public Query regexpQuery(
            String regexp,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context
        ) {
            if (this.value == null) {
                return new MatchNoDocsQuery();
            }

            final Automaton automaton = new RegExp(regexp, syntaxFlags, matchFlags).toAutomaton(maxDeterminizedStates);
            final CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
            if (runAutomaton.run(this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

    }

    ConstantKeywordFieldMapper(String simpleName, MappedFieldType mappedFieldType) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public ConstantKeywordFieldType fieldType() {
        return (ConstantKeywordFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        final String value = parser.textOrNull();

        if (value == null) {
            throw new IllegalArgumentException("[constant_keyword] field [" + name() + "] doesn't accept [null] values");
        }

        if (fieldType().value == null) {
            ConstantKeywordFieldType newFieldType = new ConstantKeywordFieldType(fieldType().name(), value, fieldType().meta());
            Mapper update = new ConstantKeywordFieldMapper(simpleName(), newFieldType);
            boolean dynamicMapperAdded = context.addDynamicMapper(update);
            // the mapper is already part of the mapping, we're just updating it with the new value
            assert dynamicMapperAdded;
        } else if (Objects.equals(fieldType().value, value) == false) {
            throw new IllegalArgumentException(
                "[constant_keyword] field ["
                    + name()
                    + "] only accepts values that are equal to the value defined in the mappings ["
                    + fieldType().value()
                    + "], but got ["
                    + value
                    + "]"
            );
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        String value = fieldType().value();
        ;
        if (value == null) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }
        return new SourceLoader.SyntheticFieldLoader() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.of();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) {
                return docId -> true;
            }

            @Override
            public boolean hasValue() {
                return true;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {
                if (fieldType().value != null) {
                    b.field(simpleName(), fieldType().value);
                }
            }
        };
    }
}
