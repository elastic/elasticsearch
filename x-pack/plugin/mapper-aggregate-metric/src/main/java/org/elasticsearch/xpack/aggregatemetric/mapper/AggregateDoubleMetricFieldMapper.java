/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;
import org.elasticsearch.xpack.aggregatemetric.fielddata.IndexAggregateDoubleMetricFieldData;
import org.elasticsearch.xpack.aggregatemetric.fielddata.LeafAggregateDoubleMetricFieldData;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/** A {@link FieldMapper} for a field containing aggregate metrics such as min/max/value_count etc. */
public class AggregateDoubleMetricFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "aggregate_metric_double";
    public static final String SUBFIELD_SEPARATOR = ".";

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
        public static final ParseField IGNORE_MALFORMED = new ParseField("ignore_malformed");
        public static final ParseField METRICS = new ParseField("metrics");
        public static final ParseField DEFAULT_METRIC = new ParseField("default_metric");
    }

    /**
     * Enum of aggregate metrics supported by this field mapper
     */
    public enum Metric {
        min,
        max,
        sum,
        value_count;
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Set<Metric>> METRICS = new Explicit<>(Collections.emptySet(), false);
        public static final Explicit<Metric> DEFAULT_METRIC = new Explicit<>(Metric.max, false);
        public static final FieldType FIELD_TYPE = new FieldType();
    }

    public static class Builder extends FieldMapper.Builder<AggregateDoubleMetricFieldMapper.Builder> {

        private Boolean ignoreMalformed;

        /**
         * The aggregated metrics supported by the field type
         */
        private EnumSet<Metric> metrics;

        /**
         * Set the default metric so that query operations are delegated to it.
         */
        private Metric defaultMetric;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public AggregateDoubleMetricFieldMapper.Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return AggregateDoubleMetricFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        public AggregateDoubleMetricFieldMapper.Builder defaultMetric(Metric defaultMetric) {
            this.defaultMetric = defaultMetric;
            return builder;
        }

        protected Explicit<Metric> defaultMetric(BuilderContext context) {
            if (defaultMetric != null) {
                if (metrics != null && metrics.contains(defaultMetric) == false) {
                    // The default_metric is not defined in the "metrics" field
                    throw new IllegalArgumentException("Metric [" + defaultMetric + "] is not defined in the metrics field.");
                }
                return new Explicit<>(defaultMetric, true);
            }

            // If a single metric is contained, this should be the default
            if (metrics != null && metrics.size() == 1) {
                return new Explicit<>(metrics.iterator().next(), false);
            }

            if (metrics.contains(Defaults.DEFAULT_METRIC.value())) {
                return Defaults.DEFAULT_METRIC;
            }
            throw new IllegalArgumentException(
                "Property [" + Names.DEFAULT_METRIC.getPreferredName() + "] must be set for field [" + name() + "]."
            );
        }

        public AggregateDoubleMetricFieldMapper.Builder metrics(EnumSet<Metric> metrics) {
            this.metrics = metrics;
            return builder;
        }

        protected Explicit<Set<Metric>> metrics(BuilderContext context) {
            if (metrics != null) {
                return new Explicit<>(metrics, true);
            }
            return Defaults.METRICS;
        }

        @Override
        public AggregateDoubleMetricFieldMapper build(BuilderContext context) {
            if (metrics == null || metrics.isEmpty()) {
                throw new IllegalArgumentException(
                    "Property [" + Names.METRICS.getPreferredName() + "] must be set for field [" + name() + "]."
                );
            }

            EnumMap<Metric, NumberFieldMapper> metricMappers = new EnumMap<>(Metric.class);
            // Instantiate one NumberFieldMapper instance for each metric
            for (Metric m : this.metrics) {
                String fieldName = subfieldName(name, m);
                NumberFieldMapper.Builder builder;

                if (m == Metric.value_count) {
                    // value_count metric can only be an integer and not a double
                    builder = new NumberFieldMapper.Builder(fieldName, NumberFieldMapper.NumberType.INTEGER);
                    builder.coerce(false);
                } else {
                    builder = new NumberFieldMapper.Builder(fieldName, NumberFieldMapper.NumberType.DOUBLE);
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
            Explicit<Metric> defaultMetric = defaultMetric(context);
            AggregateDoubleMetricFieldType metricFieldType = new AggregateDoubleMetricFieldType(buildFullName(context), hasDocValues, meta);
            metricFieldType.setMetricFields(metricFields);
            metricFieldType.setDefaultMetric(defaultMetric.value());

            return new AggregateDoubleMetricFieldMapper(
                name,
                fieldType,
                metricFieldType,
                ignoreMalformed(context),
                metrics(context),
                defaultMetric,
                metricMappers
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AggregateDoubleMetricFieldMapper.Builder builder = new AggregateDoubleMetricFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(Names.METRICS.getPreferredName())) {
                    String metricsStr[] = XContentMapValues.nodeStringArrayValue(propNode);
                    // Make sure that metrics are supported
                    EnumSet<Metric> parsedMetrics = EnumSet.noneOf(Metric.class);
                    for (int i = 0; i < metricsStr.length; i++) {
                        try {
                            Metric m = Metric.valueOf(metricsStr[i]);
                            parsedMetrics.add(m);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException("Metric [" + metricsStr[i] + "] is not supported.", e);
                        }
                    }
                    builder.metrics(parsedMetrics);
                    iterator.remove();
                } else if (propName.equals(Names.DEFAULT_METRIC.getPreferredName())) {
                    String defaultMetric = XContentMapValues.nodeStringValue(
                        propNode,
                        name + "." + Names.DEFAULT_METRIC.getPreferredName()
                    );
                    try {
                        Metric m = Metric.valueOf(defaultMetric);
                        builder.defaultMetric(m);
                        iterator.remove();
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Metric [" + defaultMetric + "] is not supported.", e);
                    }
                } else if (propName.equals(Names.IGNORE_MALFORMED.getPreferredName())) {
                    builder.ignoreMalformed(
                        XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED.getPreferredName())
                    );
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class AggregateDoubleMetricFieldType extends SimpleMappedFieldType {

        private EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields;

        private Metric defaultMetric;

        public AggregateDoubleMetricFieldType(String name) {
            this(name, false, Collections.emptyMap());
        }

        public AggregateDoubleMetricFieldType(String name, boolean hasDocValues, Map<String, String> meta) {
            super(name, false, hasDocValues, TextSearchInfo.NONE, meta);
        }

        AggregateDoubleMetricFieldType(AggregateDoubleMetricFieldType other) {
            super(other);
            this.metricFields = other.metricFields;
            this.defaultMetric = other.defaultMetric;
        }

        @Override
        public MappedFieldType clone() {
            return new AggregateDoubleMetricFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            if (super.equals(o) == false) {
                return false;
            }
            AggregateDoubleMetricFieldType other = (AggregateDoubleMetricFieldType) o;
            return Objects.equals(metricFields, other.metricFields) && defaultMetric == other.defaultMetric;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), metricFields, defaultMetric);
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

        @Override
        public Query existsQuery(QueryShardContext context) {
            return delegateFieldType().existsQuery(context);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return delegateFieldType().termQuery(value, context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            return delegateFieldType().termsQuery(values, context);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(
                    IndexSettings indexSettings,
                    MappedFieldType fieldType,
                    IndexFieldDataCache cache,
                    CircuitBreakerService breakerService,
                    MapperService mapperService
                ) {
                    return new IndexAggregateDoubleMetricFieldData(indexSettings.getIndex(), fieldType.name()) {

                        @Override
                        public ValuesSourceType getValuesSourceType() {
                            return AggregateMetricsValuesSourceType.AGGREGATE_METRIC;
                        }

                        @Override
                        public LeafAggregateDoubleMetricFieldData load(LeafReaderContext context) {
                            return new LeafAggregateDoubleMetricFieldData() {
                                @Override
                                public SortedNumericDoubleValues getAggregateMetricValues(final Metric metric) throws IOException {
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
                                        throw new IOException("Cannot load doc values", e);
                                    }
                                }

                                @Override
                                public ScriptDocValues<?> getScriptValues() {
                                    throw new UnsupportedOperationException(
                                        "The [" + CONTENT_TYPE + "] field does not " + "support scripts"
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
                            SortField sortField = new SortedNumericSortField(delegateFieldType().name(), SortField.Type.DOUBLE, reverse);
                            return sortField;
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
            };
        }
    }

    private final EnumMap<Metric, NumberFieldMapper> metricFieldMappers;

    private Explicit<Boolean> ignoreMalformed;

    /** A set of metrics supported */
    private Explicit<Set<Metric>> metrics;

    /** The default metric to be when querying this field type */
    protected Explicit<Metric> defaultMetric;

    private AggregateDoubleMetricFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Set<Metric>> metrics,
        Explicit<Metric> defaultMetric,
        EnumMap<Metric, NumberFieldMapper> metricFieldMappers
    ) {
        super(simpleName, fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        this.ignoreMalformed = ignoreMalformed;
        this.metrics = metrics;
        this.defaultMetric = defaultMetric;
        this.metricFieldMappers = metricFieldMappers;
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
    protected AggregateDoubleMetricFieldMapper clone() {
        return (AggregateDoubleMetricFieldMapper) super.clone();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>(metricFieldMappers.values());
        return mappers.iterator();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }

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
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, context.parser()::getTokenLocation);
            subParser = new XContentSubParser(context.parser());
            token = subParser.nextToken();
            while (token != XContentParser.Token.END_OBJECT) {
                // should be an object sub-field with name a metric name
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser::getTokenLocation);
                String fieldName = subParser.currentName();
                Metric metric = Metric.valueOf(fieldName);

                if (metrics.value().contains(metric) == false) {
                    throw new IllegalArgumentException(
                        "Aggregate metric [" + metric + "] does not exist in the mapping of field [" + mappedFieldType.name() + "]"
                    );
                }

                token = subParser.nextToken();
                // Make sure that the value is a number. Probably this will change when
                // new aggregate metric types are added (histogram, cardinality etc)
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser::getTokenLocation);
                NumberFieldMapper delegateFieldMapper = metricFieldMappers.get(metric);
                // We don't accept arrays of metrics
                if (context.doc().getField(delegateFieldMapper.fieldType().name()) != null) {
                    throw new IllegalArgumentException(
                        "Field ["
                            + name()
                            + "] of type ["
                            + typeName()
                            + "] does not support indexing multiple values for the same field in the same document"
                    );
                }
                // Delegate parsing the field to a numeric field mapper
                delegateFieldMapper.parse(context);

                // Ensure a value_count metric does not have a negative value
                if (Metric.value_count == metric) {
                    // context.doc().getField() method iterates over all fields in the document.
                    // Making the following call slow down. Maybe we can think something smarter.
                    Number n = context.doc().getField(delegateFieldMapper.name()).numericValue();
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
            if (metricsParsed.containsAll(metrics.value()) == false) {
                throw new IllegalArgumentException(
                    "Aggregate metric field [" + mappedFieldType.name() + "] must contain all metrics " + metrics.value().toString()
                );
            }
        } catch (Exception e) {
            if (ignoreMalformed.value()) {
                if (subParser != null) {
                    // close the subParser so we advance to the end of the object
                    subParser.close();
                }
                // If ignoreMalformed == true, clear all parsed fields
                Set<String> ignoreFieldNames = new HashSet<>(metricFieldMappers.size());
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
    protected void mergeOptions(FieldMapper mergeWith, List<String> conflicts) {
        AggregateDoubleMetricFieldMapper other = (AggregateDoubleMetricFieldMapper) mergeWith;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }

        if (other.metrics.explicit()) {
            if (this.metrics.value() != null
                && metrics.value().isEmpty() == false
                && (metrics.value().containsAll(other.metrics.value()) == false
                    || other.metrics.value().containsAll(metrics.value()) == false)) {
                conflicts.add(
                    "["
                        + fieldType().name()
                        + "] with field mapper ["
                        + fieldType().typeName()
                        + "] cannot be merged with ["
                        + other.fieldType().typeName()
                        + "] because they contain separate metrics"
                );
            }
            this.metrics = other.metrics;
        }

        if (other.defaultMetric.explicit()) {
            this.defaultMetric = other.defaultMetric;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED.getPreferredName(), ignoreMalformed.value());
        }

        if (includeDefaults || metrics.explicit()) {
            builder.field(Names.METRICS.getPreferredName(), metrics.value());
        }

        if (includeDefaults || defaultMetric.explicit()) {
            builder.field(Names.DEFAULT_METRIC.getPreferredName(), defaultMetric.value());
        }
    }
}
