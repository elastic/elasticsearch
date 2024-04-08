/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.index.mapper.IpPrefixAutomatonUtil.buildIpPrefixAutomaton;

/**
 * A {@link FieldMapper} for ip addresses.
 */
public class IpFieldMapper extends FieldMapper {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IpFieldMapper.class);

    public static final String CONTENT_TYPE = "ip";

    private static IpFieldMapper toType(FieldMapper in) {
        return (IpFieldMapper) in;
    }

    public static final class Builder extends FieldMapper.DimensionBuilder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Boolean> ignoreMalformed;
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValueAsString, null)
            .acceptsNull();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<OnScriptError> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Boolean> dimension;

        private final boolean ignoreMalformedByDefault;
        private final IndexVersion indexCreatedVersion;
        private final ScriptCompiler scriptCompiler;

        public Builder(String name, ScriptCompiler scriptCompiler, boolean ignoreMalformedByDefault, IndexVersion indexCreatedVersion) {
            super(name);
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.ignoreMalformedByDefault = ignoreMalformedByDefault;
            this.indexCreatedVersion = indexCreatedVersion;
            this.ignoreMalformed = Parameter.boolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);
            this.script.precludesParameters(nullValue, ignoreMalformed);
            addScriptValidation(script, indexed, hasDocValues);
            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).dimension).addValidator(v -> {
                if (v && (indexed.getValue() == false || hasDocValues.getValue() == false)) {
                    throw new IllegalArgumentException(
                        "Field ["
                            + TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM
                            + "] requires that ["
                            + indexed.name
                            + "] and ["
                            + hasDocValues.name
                            + "] are true"
                    );
                }
            });
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        private InetAddress parseNullValue() {
            String nullValueAsString = nullValue.getValue();
            if (nullValueAsString == null) {
                return null;
            }
            try {
                return InetAddresses.forString(nullValueAsString);
            } catch (Exception e) {
                if (indexCreatedVersion.onOrAfter(IndexVersions.V_8_0_0)) {
                    throw new MapperParsingException("Error parsing [null_value] on field [" + name() + "]: " + e.getMessage(), e);
                } else {
                    DEPRECATION_LOGGER.warn(
                        DeprecationCategory.MAPPINGS,
                        "ip_mapper_null_field",
                        "Error parsing ["
                            + nullValue.getValue()
                            + "] as IP in [null_value] on field ["
                            + name()
                            + "]); [null_value] will be ignored"
                    );
                    return null;
                }
            }
        }

        private FieldValues<InetAddress> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            IpFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), IpFieldScript.CONTEXT);
            return factory == null
                ? null
                : (lookup, ctx, doc, consumer) -> factory.newFactory(name(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, stored, ignoreMalformed, nullValue, script, onScriptError, meta, dimension };
        }

        @Override
        public IpFieldMapper build(MapperBuilderContext context) {
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension.setValue(true);
            }
            return new IpFieldMapper(
                name(),
                new IpFieldType(
                    context.buildFullName(name()),
                    indexed.getValue() && indexCreatedVersion.isLegacyIndexVersion() == false,
                    stored.getValue(),
                    hasDocValues.getValue(),
                    parseNullValue(),
                    scriptValues(),
                    meta.getValue(),
                    dimension.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo,
                context.isSourceSynthetic(),
                this
            );
        }

    }

    private static final IndexVersion MINIMUM_COMPATIBILITY_VERSION = IndexVersion.fromId(5000099);

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(n, c.scriptCompiler(), ignoreMalformedByDefault, c.indexVersionCreated());
    }, MINIMUM_COMPATIBILITY_VERSION);

    public static final class IpFieldType extends SimpleMappedFieldType {

        private final InetAddress nullValue;
        private final FieldValues<InetAddress> scriptValues;
        private final boolean isDimension;

        public IpFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            InetAddress nullValue,
            FieldValues<InetAddress> scriptValues,
            Map<String, String> meta,
            boolean isDimension
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
            this.isDimension = isDimension;
        }

        public IpFieldType(String name) {
            this(name, true, true);
        }

        public IpFieldType(String name, boolean isIndexed) {
            this(name, isIndexed, true);
        }

        public IpFieldType(String name, boolean isIndexed, boolean hasDocValues) {
            this(name, isIndexed, false, hasDocValues, null, null, Collections.emptyMap(), false);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return isIndexed() || hasDocValues();
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return context.fieldExistsInIndex(name());
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public boolean hasScriptValues() {
            return scriptValues != null;
        }

        private static InetAddress parse(Object value) {
            if (value instanceof InetAddress) {
                return (InetAddress) value;
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return InetAddresses.forString(value.toString());
            }
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (scriptValues != null) {
                return FieldValues.valueFetcher(scriptValues, v -> InetAddresses.toAddrString((InetAddress) v), context);
            }
            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    InetAddress address;
                    if (value instanceof InetAddress) {
                        address = (InetAddress) value;
                    } else {
                        address = InetAddresses.forString(value.toString());
                    }
                    return InetAddresses.toAddrString(address);
                }
            };
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            Query query;
            if (value instanceof InetAddress) {
                query = InetAddressPoint.newExactQuery(name(), (InetAddress) value);
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                String term = value.toString();
                if (term.contains("/")) {
                    final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(term);
                    query = InetAddressPoint.newPrefixQuery(name(), cidr.v1(), cidr.v2());
                } else {
                    InetAddress address = InetAddresses.forString(term);
                    query = InetAddressPoint.newExactQuery(name(), address);
                }
            }
            if (isIndexed()) {
                return query;
            } else {
                return convertToDocValuesQuery(query);
            }
        }

        static Query convertToDocValuesQuery(Query query) {
            assert query instanceof PointRangeQuery;
            PointRangeQuery pointRangeQuery = (PointRangeQuery) query;
            return SortedSetDocValuesField.newSlowRangeQuery(
                pointRangeQuery.getField(),
                new BytesRef(pointRangeQuery.getLowerPoint()),
                new BytesRef(pointRangeQuery.getUpperPoint()),
                true,
                true
            );
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (isIndexed() == false) {
                return super.termsQuery(values, context);
            }
            InetAddress[] addresses = new InetAddress[values.size()];
            int i = 0;
            for (Object value : values) {
                InetAddress address;
                if (value instanceof InetAddress) {
                    address = (InetAddress) value;
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    if (value.toString().contains("/")) {
                        // the `terms` query contains some prefix queries, so we cannot create a set query
                        // and need to fall back to a disjunction of `term` queries
                        return super.termsQuery(values, context);
                    }
                    address = InetAddresses.forString(value.toString());
                }
                addresses[i++] = address;
            }
            return InetAddressPoint.newSetQuery(name(), addresses);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (lower, upper) -> {
                Query query = InetAddressPoint.newRangeQuery(name(), lower, upper);
                if (isIndexed()) {
                    if (hasDocValues()) {
                        return new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query));
                    } else {
                        return query;
                    }
                } else {
                    return convertToDocValuesQuery(query);
                }
            });
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<InetAddress, InetAddress, Query> builder
        ) {
            InetAddress lower;
            if (lowerTerm == null) {
                lower = InetAddressPoint.MIN_VALUE;
            } else {
                lower = parse(lowerTerm);
                if (includeLower == false) {
                    if (lower.equals(InetAddressPoint.MAX_VALUE)) {
                        return new MatchNoDocsQuery();
                    }
                    lower = InetAddressPoint.nextUp(lower);
                }
            }

            InetAddress upper;
            if (upperTerm == null) {
                upper = InetAddressPoint.MAX_VALUE;
            } else {
                upper = parse(upperTerm);
                if (includeUpper == false) {
                    if (upper.equals(InetAddressPoint.MIN_VALUE)) {
                        return new MatchNoDocsQuery();
                    }
                    upper = InetAddressPoint.nextDown(upper);
                }
            }

            return builder.apply(lower, upper);
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (hasDocValues()) {
                return new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader(name());
            }
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.IP, IpDocValuesField::new);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return DocValueFormat.IP.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            checkNoFormat(format);
            checkNoTimeZone(timeZone);
            return DocValueFormat.IP;
        }

        @Override
        public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) throws IOException {

            Terms terms = null;
            // terms_enum for ip only works if doc values are enabled
            if (hasDocValues()) {
                terms = SortedSetDocValuesTerms.getTerms(reader, name());
            }
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }
            BytesRef searchBytes = searchAfter == null ? null : new BytesRef(InetAddressPoint.encode(InetAddress.getByName(searchAfter)));
            CompiledAutomaton prefixAutomaton = buildIpPrefixAutomaton(prefix);

            if (prefixAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.ALL) {
                TermsEnum result = terms.iterator();
                if (searchAfter != null) {
                    result = new SearchAfterTermsEnum(result, searchBytes);
                }
                return result;
            }
            return terms.intersect(prefixAutomaton, searchBytes);
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final boolean ignoreMalformed;
    private final boolean storeIgnored;
    private final boolean dimension;

    private final InetAddress nullValue;
    private final String nullValueAsString;

    private final boolean ignoreMalformedByDefault;
    private final IndexVersion indexCreatedVersion;

    private final Script script;
    private final FieldValues<InetAddress> scriptValues;
    private final ScriptCompiler scriptCompiler;

    private IpFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean storeIgnored,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, builder.script.get() != null, builder.onScriptError.get());
        this.ignoreMalformedByDefault = builder.ignoreMalformedByDefault;
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.nullValue = builder.parseNullValue();
        this.nullValueAsString = builder.nullValue.getValue();
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.script = builder.script.get();
        this.scriptValues = builder.scriptValues();
        this.scriptCompiler = builder.scriptCompiler;
        this.dimension = builder.dimension.getValue();
        this.storeIgnored = storeIgnored;
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed;
    }

    @Override
    public IpFieldType fieldType() {
        return (IpFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        InetAddress address;
        try {
            address = value(context.parser(), nullValue);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed) {
                context.addIgnoredField(fieldType().name());
                if (storeIgnored) {
                    // Save a copy of the field so synthetic source can load it
                    context.doc().add(IgnoreMalformedStoredValues.storedField(name(), context.parser()));
                }
                return;
            } else {
                throw e;
            }
        }
        if (address != null) {
            indexValue(context, address);
        }
    }

    private static InetAddress value(XContentParser parser, InetAddress nullValue) throws IOException {
        String value = parser.textOrNull();
        if (value == null) {
            return nullValue;
        }
        return InetAddresses.forString(value);
    }

    private void indexValue(DocumentParserContext context, InetAddress address) {
        if (dimension) {
            context.getDimensions().addIp(fieldType().name(), address).validate(context.indexSettings());
        }
        if (indexed) {
            Field field = new InetAddressPoint(fieldType().name(), address);
            context.doc().add(field);
        }
        if (hasDocValues) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        } else if (stored || indexed) {
            context.addToFieldNames(fieldType().name());
        }
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), scriptCompiler, ignoreMalformedByDefault, indexCreatedVersion).dimension(dimension).init(this);
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (dimension && null != lookup.nestedLookup().getNestedParent(name())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + name() + "]"
            );
        }
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (hasScript()) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }
        if (hasDocValues == false) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it doesn't have doc values"
            );
        }
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        return new SortedSetDocValuesSyntheticFieldLoader(name(), simpleName(), null, ignoreMalformed) {
            @Override
            protected BytesRef convert(BytesRef value) {
                byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
                return new BytesRef(NetworkAddress.format(InetAddressPoint.decode(bytes)));
            }

            @Override
            protected BytesRef preserve(BytesRef value) {
                // No need to copy because convert has made a deep copy
                return value;
            }
        };
    }
}
