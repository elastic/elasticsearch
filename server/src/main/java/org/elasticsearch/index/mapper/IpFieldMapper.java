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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** A {@link FieldMapper} for ip addresses. */
public class IpFieldMapper extends FieldMapper {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IpFieldMapper.class);

    public static final String CONTENT_TYPE = "ip";

    private static IpFieldMapper toType(FieldMapper in) {
        return (IpFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Boolean> ignoreMalformed;
        private final Parameter<String> nullValue
            = Parameter.stringParam("null_value", false, m -> toType(m).nullValueAsString, null).acceptsNull();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Boolean> dimension;

        private final boolean ignoreMalformedByDefault;
        private final Version indexCreatedVersion;
        private final ScriptCompiler scriptCompiler;

        public Builder(String name, ScriptCompiler scriptCompiler, boolean ignoreMalformedByDefault, Version indexCreatedVersion) {
            super(name);
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.ignoreMalformedByDefault = ignoreMalformedByDefault;
            this.indexCreatedVersion = indexCreatedVersion;
            this.ignoreMalformed
                = Parameter.boolParam("ignore_malformed", true, m -> toType(m).ignoreMalformed, ignoreMalformedByDefault);
            this.script.precludesParameters(nullValue, ignoreMalformed);
            addScriptValidation(script, indexed, hasDocValues);
            this.dimension = Parameter.boolParam("dimension", false, m -> toType(m).dimension, false)
                .setValidator(v -> {
                    if (v && (indexed.getValue() == false || hasDocValues.getValue() == false)) {
                        throw new IllegalArgumentException(
                            "Field [dimension] requires that [" + indexed.name + "] and [" + hasDocValues.name + "] are true"
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
                if (indexCreatedVersion.onOrAfter(Version.V_8_0_0)) {
                    throw new MapperParsingException("Error parsing [null_value] on field [" + name() + "]: " + e.getMessage(), e);
                } else {
                    DEPRECATION_LOGGER.deprecate(DeprecationCategory.MAPPINGS, "ip_mapper_null_field", "Error parsing [" +
                        nullValue.getValue() + "] as IP in [null_value] on field [" + name() + "]); [null_value] will be ignored");
                    return null;
                }
            }
        }

        private FieldValues<InetAddress> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            IpFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), IpFieldScript.CONTEXT);
            return factory == null ? null : (lookup, ctx, doc, consumer) -> factory
                .newFactory(name, script.get().getParams(), lookup)
                .newInstance(ctx)
                .runForDoc(doc, consumer);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(indexed, hasDocValues, stored, ignoreMalformed, nullValue, script, onScriptError, meta, dimension);
        }

        @Override
        public IpFieldMapper build(ContentPath contentPath) {
            return new IpFieldMapper(name,
                new IpFieldType(buildFullName(contentPath), indexed.getValue(), stored.getValue(),
                    hasDocValues.getValue(), parseNullValue(), scriptValues(), meta.getValue(), dimension.getValue()),
                multiFieldsBuilder.build(this, contentPath), copyTo.build(), this);
        }

    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        boolean ignoreMalformedByDefault = IGNORE_MALFORMED_SETTING.get(c.getSettings());
        return new Builder(n, c.scriptCompiler(), ignoreMalformedByDefault, c.indexVersionCreated());
    });

    public static final class IpFieldType extends SimpleMappedFieldType {

        private final InetAddress nullValue;
        private final FieldValues<InetAddress> scriptValues;
        private final boolean isDimension;

        public IpFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                           InetAddress nullValue, FieldValues<InetAddress> scriptValues, Map<String, String> meta, boolean isDimension) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
            this.isDimension = isDimension;
        }

        public IpFieldType(String name) {
            this(name, true, false, true, null, null, Collections.emptyMap(), false);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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
                return FieldValues.valueFetcher(scriptValues, v -> InetAddresses.toAddrString((InetAddress)v), context);
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
            failIfNotIndexed();
            if (value instanceof InetAddress) {
                return InetAddressPoint.newExactQuery(name(), (InetAddress) value);
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                String term = value.toString();
                if (term.contains("/")) {
                    final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(term);
                    return InetAddressPoint.newPrefixQuery(name(), cidr.v1(), cidr.v2());
                }
                InetAddress address = InetAddresses.forString(term);
                return InetAddressPoint.newExactQuery(name(), address);
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
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
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                                SearchExecutionContext context) {
            failIfNotIndexed();
            return rangeQuery(
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                (lower, upper) -> InetAddressPoint.newRangeQuery(name(), lower, upper)
            );
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

        public static final class IpScriptDocValues extends ScriptDocValues<String> {

            private final SortedSetDocValues in;
            private long[] ords = new long[0];
            private int count;

            public IpScriptDocValues(SortedSetDocValues in) {
                this.in = in;
            }

            @Override
            public void setNextDocId(int docId) throws IOException {
                count = 0;
                if (in.advanceExact(docId)) {
                    for (long ord = in.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = in.nextOrd()) {
                        ords = ArrayUtil.grow(ords, count + 1);
                        ords[count++] = ord;
                    }
                }
            }

            public String getValue() {
                if (count == 0) {
                    return null;
                } else {
                    return get(0);
                }
            }

            @Override
            public String get(int index) {
                try {
                    BytesRef encoded = in.lookupOrd(ords[index]);
                    InetAddress address = InetAddressPoint.decode(
                            Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
                    return InetAddresses.toAddrString(address);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int size() {
                return count;
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), IpScriptDocValues::new, CoreValuesSourceType.IP);
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
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            return DocValueFormat.IP;
        }

        /**
         * @return true if field has been marked as a dimension field
         */
        public boolean isDimension() {
            return isDimension;
        }
    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final boolean ignoreMalformed;
    private final boolean dimension;

    private final InetAddress nullValue;
    private final String nullValueAsString;

    private final boolean ignoreMalformedByDefault;
    private final Version indexCreatedVersion;

    private final Script script;
    private final FieldValues<InetAddress> scriptValues;
    private final ScriptCompiler scriptCompiler;

    private IpFieldMapper(
            String simpleName,
            MappedFieldType mappedFieldType,
            MultiFields multiFields,
            CopyTo copyTo,
            Builder builder) {
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
    }

    boolean ignoreMalformed() {
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
        if (indexed) {
            Field field = new InetAddressPoint(fieldType().name(), address);
            if (dimension) {
                // Add dimension field with key so that we ensure it is single-valued.
                // Dimension fields are always indexed.
                if (context.doc().getByKey(fieldType().name()) != null) {
                    throw new IllegalArgumentException("Dimension field [" + fieldType().name() + "] cannot be a multi-valued field.");
                }
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                context.doc().add(field);
            }
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
    protected void indexScriptValues(SearchLookup searchLookup, LeafReaderContext readerContext, int doc,
                                     DocumentParserContext documentParserContext) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), scriptCompiler, ignoreMalformedByDefault, indexCreatedVersion).dimension(dimension).init(this);
    }
}
