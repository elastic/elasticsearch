/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.DoublesSupplier;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SortedNumericDocValuesSyntheticFieldLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;
import org.elasticsearch.xpack.aggregatemetric.fielddata.IndexAggregateDoubleMetricFieldData;
import org.elasticsearch.xpack.aggregatemetric.fielddata.LeafAggregateDoubleMetricFieldData;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/** A {@link FieldMapper} for a field containing aggregate metrics such as min/max/value_count etc. */
public class AggregateDoubleMetricFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "aggregate_metric_double";
    public static final String SUBFIELD_SEPARATOR = ".";

    private static AggregateDoubleMetricFieldMapper toType(FieldMapper in) {
        return (AggregateDoubleMetricFieldMapper) in;
    }

    /**
     * Return the name of a subfield of an aggregate metric field
     *
     * @param fieldName the name of the aggregate metric field
     * @param metric    the metric type the subfield corresponds to
     * @return the name of the subfield
     */
    public static String subfieldName(String fieldName, Metric metric) {
        return fieldName + AggregateDoubleMetricFieldMapper.SUBFIELD_SEPARATOR + metric.name();
    }

    /**
     * Mapping field names
     */
    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
        public static final String METRICS = "metrics";
        public static final String DEFAULT_METRIC = "default_metric";
    }

    /**
     * Enum of aggregate metrics supported by this field mapper
     */
    public enum Metric {
        min,
        max,
        sum,
        value_count
    }

    public static class Defaults {
        public static final EnumSet<Metric> METRICS = EnumSet.noneOf(Metric.class);
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<Boolean> ignoreMalformed;

        private final Parameter<EnumSet<Metric>> metrics = new Parameter<>(Names.METRICS, false, () -> Defaults.METRICS, (n, c, o) -> {
            @SuppressWarnings("unchecked")
            List<String> metricsList = (List<String>) o;
            EnumSet<Metric> parsedMetrics = EnumSet.noneOf(Metric.class);
            for (String s : metricsList) {
                try {
                    Metric m = Metric.valueOf(s);
                    parsedMetrics.add(m);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Metric [" + s + "] is not supported.", e);
                }
            }
            return parsedMetrics;
        }, m -> toType(m).metrics, XContentBuilder::enumSet, Objects::toString).addValidator(v -> {
            if (v == null || v.isEmpty()) {
                throw new IllegalArgumentException("Property [" + Names.METRICS + "] is required for field [" + name() + "].");
            }
        });

        /**
         * Parameter that marks this field as a time series metric defining its time series metric type.
         * For {@link AggregateDoubleMetricFieldMapper} fields gauge, counter and summary metric types are
         * supported.
         */
        private final Parameter<MetricType> timeSeriesMetric;

        /**
         * Set the default metric so that query operations are delegated to it.
         */
        private final Parameter<Metric> defaultMetric = new Parameter<>(Names.DEFAULT_METRIC, false, () -> null, (n, c, o) -> {
            try {
                return Metric.valueOf(o.toString());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Metric [" + o.toString() + "] is not supported.", e);
            }
        }, m -> toType(m).defaultMetric, XContentBuilder::field, Objects::toString);

        private final Version indexCreatedVersion;

        public Builder(String name, Boolean ignoreMalformedByDefault, Version indexCreatedVersion) {
            super(name);
            this.ignoreMalformed = Parameter.boolParam(
                Names.IGNORE_MALFORMED,
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );

            this.timeSeriesMetric = TimeSeriesParams.metricParam(
                m -> toType(m).metricType,
                MetricType.gauge,
                MetricType.counter,
                MetricType.summary
            );

            this.indexCreatedVersion = Objects.requireNonNull(indexCreatedVersion);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { ignoreMalformed, metrics, defaultMetric, meta, timeSeriesMetric };
        }

        public Builder metric(MetricType metric) {
            this.timeSeriesMetric.setValue(metric);
            return this;
        }

        @Override
        public AggregateDoubleMetricFieldMapper build(MapperBuilderContext context) {
            if (defaultMetric.isConfigured() == false) {
                // If a single metric is contained, this should be the default
                if (metrics.getValue().size() == 1) {
                    Metric m = metrics.getValue().iterator().next();
                    defaultMetric.setValue(m);
                }

                if (metrics.getValue().contains(defaultMetric.getValue()) == false) {
                    throw new IllegalArgumentException("Property [" + Names.DEFAULT_METRIC + "] is required for field [" + name() + "].");
                }
            }

            if (metrics.getValue().contains(defaultMetric.getValue()) == false) {
                // The default_metric is not defined in the "metrics" field
                throw new IllegalArgumentException(
                    "Default metric [" + defaultMetric.getValue() + "] is not defined in the metrics of field [" + name() + "]."
                );
            }

            EnumMap<Metric, NumberFieldMapper> metricMappers = new EnumMap<>(Metric.class);
            // Instantiate one NumberFieldMapper instance for each metric
            for (Metric m : this.metrics.getValue()) {
                String fieldName = subfieldName(name, m);
                NumberFieldMapper.Builder builder;

                if (m == Metric.value_count) {
                    // value_count metric can only be an integer and not a double
                    builder = new NumberFieldMapper.Builder(
                        fieldName,
                        NumberFieldMapper.NumberType.INTEGER,
                        ScriptCompiler.NONE,
                        false,
                        false,
                        indexCreatedVersion
                    ).allowMultipleValues(false);
                } else {
                    builder = new NumberFieldMapper.Builder(
                        fieldName,
                        NumberFieldMapper.NumberType.DOUBLE,
                        ScriptCompiler.NONE,
                        false,
                        true,
                        indexCreatedVersion
                    );
                }
                NumberFieldMapper fieldMapper = builder.build(context);
                metricMappers.put(m, fieldMapper);
            }

            EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields = metricMappers.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().fieldType(),
                        (l, r) -> { throw new IllegalArgumentException("Duplicate keys " + l + "and " + r + "."); },
                        () -> new EnumMap<>(Metric.class)
                    )
                );

            AggregateDoubleMetricFieldType metricFieldType = new AggregateDoubleMetricFieldType(
                context.buildFullName(name),
                meta.getValue(),
                timeSeriesMetric.getValue()
            );
            metricFieldType.setMetricFields(metricFields);
            metricFieldType.setDefaultMetric(defaultMetric.getValue());

            return new AggregateDoubleMetricFieldMapper(name, metricFieldType, metricMappers, this);
        }
    }

    public static final FieldMapper.TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings()), c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    public static final class AggregateDoubleMetricFieldType extends SimpleMappedFieldType {

        private EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields;

        private Metric defaultMetric;

        private final MetricType metricType;

        public AggregateDoubleMetricFieldType(String name) {
            this(name, Collections.emptyMap(), null);
        }

        public AggregateDoubleMetricFieldType(String name, Map<String, String> meta, MetricType metricType) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
            this.metricType = metricType;
        }

        /**
         * Return a delegate field type for a given metric sub-field
         * @return a field type
         */
        private NumberFieldMapper.NumberFieldType delegateFieldType(Metric metric) {
            return metricFields.get(metric);
        }

        /**
         * Return a delegate field type for the default metric sub-field
         * @return a field type
         */
        private NumberFieldMapper.NumberFieldType delegateFieldType() {
            return delegateFieldType(defaultMetric);
        }

        @Override
        public String familyTypeName() {
            return NumberFieldMapper.NumberType.DOUBLE.typeName();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private void setMetricFields(EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields) {
            this.metricFields = metricFields;
        }

        public void addMetricField(Metric m, NumberFieldMapper.NumberFieldType subfield) {
            if (metricFields == null) {
                metricFields = new EnumMap<>(AggregateDoubleMetricFieldMapper.Metric.class);
            }

            if (name() == null) {
                throw new IllegalArgumentException("Field of type [" + typeName() + "] must have a name before adding a subfield");
            }
            metricFields.put(m, subfield);
        }

        public void setDefaultMetric(Metric defaultMetric) {
            this.defaultMetric = defaultMetric;
        }

        Metric getDefaultMetric() {
            return defaultMetric;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return delegateFieldType().mayExistInIndex(context);    // TODO how does searching actually work here?
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return delegateFieldType().existsQuery(context);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            if (value == null) {
                throw new IllegalArgumentException("Cannot search for null.");
            }
            return delegateFieldType().termQuery(value, context);
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            return delegateFieldType().termsQuery(values, context);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            return delegateFieldType().rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
        }

        @Override
        public Object valueForDisplay(Object value) {
            return delegateFieldType().valueForDisplay(value);
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return delegateFieldType().docValueFormat(format, timeZone);
        }

        @Override
        public Relation isFieldWithinQuery(
            IndexReader reader,
            Object from,
            Object to,
            boolean includeLower,
            boolean includeUpper,
            ZoneId timeZone,
            DateMathParser dateMathParser,
            QueryRewriteContext context
        ) throws IOException {
            return delegateFieldType().isFieldWithinQuery(reader, from, to, includeLower, includeUpper, timeZone, dateMathParser, context);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return (cache, breakerService) -> new IndexAggregateDoubleMetricFieldData(
                name(),
                AggregateMetricsValuesSourceType.AGGREGATE_METRIC
            ) {
                @Override
                public LeafAggregateDoubleMetricFieldData load(LeafReaderContext context) {
                    return new LeafAggregateDoubleMetricFieldData() {
                        @Override
                        public SortedNumericDoubleValues getAggregateMetricValues(final Metric metric) {
                            try {
                                final SortedNumericDocValues values = DocValues.getSortedNumeric(
                                    context.reader(),
                                    subfieldName(getFieldName(), metric)
                                );

                                return new SortedNumericDoubleValues() {
                                    @Override
                                    public int docValueCount() {
                                        return values.docValueCount();
                                    }

                                    @Override
                                    public boolean advanceExact(int doc) throws IOException {
                                        return values.advanceExact(doc);
                                    }

                                    @Override
                                    public double nextValue() throws IOException {
                                        long v = values.nextValue();
                                        if (metric == Metric.value_count) {
                                            // Only value_count metrics are encoded as integers
                                            return v;
                                        } else {
                                            // All other metrics are encoded as doubles
                                            return NumericUtils.sortableLongToDouble(v);
                                        }
                                    }
                                };
                            } catch (IOException e) {
                                throw new IllegalStateException("Cannot load doc values", e);
                            }
                        }

                        @Override
                        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
                            // getAggregateMetricValues returns all metric as doubles, including `value_count`
                            return new DelegateDocValuesField(
                                new ScriptDocValues.Doubles(new DoublesSupplier(getAggregateMetricValues(defaultMetric))),
                                name
                            );
                        }

                        @Override
                        public SortedBinaryDocValues getBytesValues() {
                            throw new UnsupportedOperationException(
                                "String representation of doc values " + "for [" + CONTENT_TYPE + "] fields is not supported"
                            );
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0; // Unknown
                        }

                        @Override
                        public void close() {}
                    };
                }

                @Override
                public LeafAggregateDoubleMetricFieldData loadDirect(LeafReaderContext context) {
                    return load(context);
                }

                @Override
                public SortField sortField(
                    Object missingValue,
                    MultiValueMode sortMode,
                    XFieldComparatorSource.Nested nested,
                    boolean reverse
                ) {
                    return new SortedNumericSortField(delegateFieldType().name(), SortField.Type.DOUBLE, reverse);
                }

                @Override
                public BucketedSort newBucketedSort(
                    BigArrays bigArrays,
                    Object missingValue,
                    MultiValueMode sortMode,
                    XFieldComparatorSource.Nested nested,
                    SortOrder sortOrder,
                    DocValueFormat format,
                    int bucketSize,
                    BucketedSort.ExtraData extra
                ) {
                    throw new IllegalArgumentException("Can't sort on the [" + CONTENT_TYPE + "] field");
                }
            };
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context) {
                @Override
                @SuppressWarnings("unchecked")
                protected Object parseSourceValue(Object value) {
                    Map<String, Double> metrics = (Map<String, Double>) value;
                    return metrics.get(defaultMetric.name());
                }
            };
        }

        /**
         * If field is a time series metric field, returns its metric type
         * @return the metric type or null
         */
        public MetricType getMetricType() {
            return metricType;
        }
    }

    private final EnumMap<Metric, NumberFieldMapper> metricFieldMappers;

    private final boolean ignoreMalformed;

    private final boolean ignoreMalformedByDefault;

    private final Version indexCreatedVersion;

    /** A set of metrics supported */
    private final EnumSet<Metric> metrics;

    /** The default metric to be when querying this field type */
    protected Metric defaultMetric;

    /** The metric type (gauge, counter, summary) if  field is a time series metric */
    private final TimeSeriesParams.MetricType metricType;

    private AggregateDoubleMetricFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        EnumMap<Metric, NumberFieldMapper> metricFieldMappers,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue();
        this.metrics = builder.metrics.getValue();
        this.defaultMetric = builder.defaultMetric.getValue();
        this.metricFieldMappers = metricFieldMappers;
        this.metricType = builder.timeSeriesMetric.getValue();
        this.indexCreatedVersion = builder.indexCreatedVersion;
    }

    boolean ignoreMalformed() {
        return ignoreMalformed;
    }

    Metric defaultMetric() {
        return defaultMetric;
    }

    @Override
    public AggregateDoubleMetricFieldType fieldType() {
        return (AggregateDoubleMetricFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        context.path().add(simpleName());
        XContentParser.Token token;
        XContentSubParser subParser = null;
        EnumSet<Metric> metricsParsed = EnumSet.noneOf(Metric.class);
        try {
            token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                context.path().remove();
                return;
            }
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser());
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be an object sub-field with name a metric name
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
                String fieldName = subParser.currentName();
                Metric metric = Metric.valueOf(fieldName);

                if (metrics.contains(metric) == false) {
                    throw new IllegalArgumentException(
                        "Aggregate metric [" + metric + "] does not exist in the mapping of field [" + mappedFieldType.name() + "]"
                    );
                }

                token = subParser.nextToken();
                // Make sure that the value is a number. Probably this will change when
                // new aggregate metric types are added (histogram, cardinality etc)
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                NumberFieldMapper delegateFieldMapper = metricFieldMappers.get(metric);
                // Delegate parsing the field to a numeric field mapper
                try {
                    delegateFieldMapper.parse(context);
                } catch (MapperParsingException e) {
                    // when parse multiple values, it will throw IllegalStateException
                    // in org.apache.lucene.document.Document.onlyAddKey
                    if (e.getCause() instanceof IllegalStateException) {
                        throw new IllegalArgumentException(
                            "Field ["
                                + name()
                                + "] of type ["
                                + typeName()
                                + "] does not support indexing multiple values for the same field in the same document"
                        );
                    } else if (e.getCause()instanceof IOException exception) {
                        // throw the cause of the exception
                        throw exception;
                    } else {
                        throw e;
                    }
                }
                // Ensure a value_count metric does not have a negative value
                if (Metric.value_count == metric) {
                    // context.doc().getField() method iterates over all fields in the document.
                    // Making the following call slow down. Maybe we can think something smarter.
                    Number n = context.doc().getByKey(delegateFieldMapper.name()).numericValue();
                    if (n.intValue() < 0) {
                        throw new IllegalArgumentException(
                            "Aggregate metric [" + metric.name() + "] of field [" + mappedFieldType.name() + "] cannot be a negative number"
                        );
                    }
                }
                metricsParsed.add(metric);
                token = subParser.nextToken();
            }

            // Check if all required metrics have been parsed.
            if (metricsParsed.containsAll(metrics) == false) {
                throw new IllegalArgumentException(
                    "Aggregate metric field [" + mappedFieldType.name() + "] must contain all metrics " + metrics.toString()
                );
            }
        } catch (Exception e) {
            if (ignoreMalformed) {
                if (subParser != null) {
                    // close the subParser so we advance to the end of the object
                    subParser.close();
                }
                // If ignoreMalformed == true, clear all parsed fields
                Set<String> ignoreFieldNames = Sets.newHashSetWithExpectedSize(metricFieldMappers.size());
                for (NumberFieldMapper m : metricFieldMappers.values()) {
                    context.addIgnoredField(m.fieldType().name());
                    ignoreFieldNames.add(m.fieldType().name());
                }
                // Parsing a metric sub-field is delegated to the delegate field mapper by calling method
                // delegateFieldMapper.parse(context). Unfortunately, this method adds the parsed sub-field
                // to the document automatically. So, at this point we must undo this by removing all metric
                // sub-fields from the document. To do so, we iterate over the document fields and remove
                // the ones whose names match.
                for (Iterator<IndexableField> iter = context.doc().getFields().iterator(); iter.hasNext();) {
                    IndexableField field = iter.next();
                    if (ignoreFieldNames.contains(field.name())) {
                        iter.remove();
                    }
                }
            } else {
                // Rethrow exception as is. It is going to be caught and nested in a MapperParsingException
                // by its FieldMapper.MappedFieldType#parse()
                throw e;
            }
        }
        context.path().remove();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), ignoreMalformedByDefault, indexCreatedVersion).metric(metricType).init(this);
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (ignoreMalformed) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it ignores malformed numbers"
            );
        }
        return new AggregateMetricSyntheticFieldLoader(name(), simpleName(), metrics);
    }

    public static class AggregateMetricSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final String name;
        private final String simpleName;
        private final EnumSet<Metric> metrics;
        private final Map<Metric, SortedNumericDocValues> metricDocValues = new EnumMap<>(Metric.class);
        private final Set<Metric> metricHasValue = EnumSet.noneOf(Metric.class);

        protected AggregateMetricSyntheticFieldLoader(String name, String simpleName, EnumSet<Metric> metrics) {
            this.name = name;
            this.simpleName = simpleName;
            this.metrics = metrics;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            metricDocValues.clear();
            for (Metric m : metrics) {
                String fieldName = subfieldName(name, m);
                SortedNumericDocValues dv = SortedNumericDocValuesSyntheticFieldLoader.docValuesOrNull(reader, fieldName);
                if (dv != null) {
                    metricDocValues.put(m, dv);
                }
            }

            if (metricDocValues.isEmpty()) {
                return null;
            }

            return new AggregateDocValuesLoader();
        }

        @Override
        public boolean hasValue() {
            return metricHasValue.isEmpty() == false;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (metricHasValue.isEmpty()) {
                return;
            }
            b.startObject(simpleName);
            for (Map.Entry<Metric, SortedNumericDocValues> entry : metricDocValues.entrySet()) {
                if (metricHasValue.contains(entry.getKey())) {
                    String metricName = entry.getKey().name();
                    long value = entry.getValue().nextValue();
                    if (entry.getKey() == Metric.value_count) {
                        b.field(metricName, value);
                    } else {
                        b.field(metricName, NumericUtils.sortableLongToDouble(value));
                    }
                }
            }
            b.endObject();
        }

        private class AggregateDocValuesLoader implements DocValuesLoader {
            @Override
            public boolean advanceToDoc(int docId) throws IOException {
                // It is required that all defined metrics must exist. In this case
                // it is enough to check for the first docValue. However, in the future
                // we may relax the requirement of all metrics existing. In this case
                // we should check the doc value for each metric separately
                metricHasValue.clear();
                for (Map.Entry<Metric, SortedNumericDocValues> e : metricDocValues.entrySet()) {
                    if (e.getValue().advanceExact(docId)) {
                        metricHasValue.add(e.getKey());
                    }
                }

                return metricHasValue.isEmpty() == false;
            }
        }
    }
}
