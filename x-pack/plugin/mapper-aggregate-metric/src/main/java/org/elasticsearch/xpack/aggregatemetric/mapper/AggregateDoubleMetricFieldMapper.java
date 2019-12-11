/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;


import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A {@link FieldMapper} for a field containing aggregate metrics such as min/max/value_count etc. */
public class AggregateDoubleMetricFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "aggregate_metric_double";

    /**
     * Mapping field names
     */
    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
        public static final ParseField METRICS = new ParseField("metrics");
    }

    /**
     * Enum of aggregate metrics supported by this field mapper
     */
    enum Metrics {
        min, max, sum, value_count;
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Set<Metrics>> METRICS = new Explicit<>(Collections.emptySet(), false);
        public static final AggregateMetricFieldType FIELD_TYPE = new AggregateMetricFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<AggregateDoubleMetricFieldMapper.Builder, AggregateDoubleMetricFieldMapper> {

        private Boolean ignoreMalformed;

        private EnumSet<Metrics> metrics;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
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

        public AggregateDoubleMetricFieldMapper.Builder metrics(EnumSet<Metrics> metrics) {
            this.metrics = metrics;
            return builder;
        }

        protected Explicit<Set<Metrics>> metrics(BuilderContext context) {
            if (metrics != null) {
                return new Explicit<Set<Metrics>>(metrics, true);
            }
            return Defaults.METRICS;
        }

        @Override
        public AggregateDoubleMetricFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new AggregateDoubleMetricFieldMapper(name, fieldType, defaultFieldType,
                context.indexSettings(), multiFieldsBuilder.build(this, context),
                ignoreMalformed(context), metrics(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<Builder, AggregateDoubleMetricFieldMapper> parse(String name,
                                                                               Map<String, Object> node,
                                                                               ParserContext parserContext) throws MapperParsingException {
            AggregateDoubleMetricFieldMapper.Builder builder = new AggregateDoubleMetricFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(Names.METRICS.getPreferredName())) {
                    String metricsStr[] = XContentMapValues.nodeStringArrayValue(propNode);
                    // Make sure that metrics are supported
                    EnumSet<Metrics> parsedMetrics = EnumSet.noneOf(Metrics.class);
                    for (int i = 0; i < metricsStr.length; i++) {
                        try {
                            Metrics m = Metrics.valueOf(metricsStr[i]);
                            parsedMetrics.add(m);
                        } catch (IllegalArgumentException e) {
                            throw new MapperParsingException("Metric [" + metricsStr[i] + "] is not supported.");
                        }
                    }
                    builder.metrics(parsedMetrics);
                    iterator.remove();
                } else if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                } else if (TypeParsers.parseMultiField(builder, name, parserContext, propName, propNode)) {
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class AggregateMetricFieldType extends MappedFieldType {

        public AggregateMetricFieldType() {
        }

        AggregateMetricFieldType(AggregateMetricFieldType other) {
            super(other);
        }

        @Override
        public MappedFieldType clone() {
            return new AggregateMetricFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues() == true) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                throw new QueryShardException(context, "field  " + name() + " of type [" + CONTENT_TYPE + "] " +
                    "has no doc values and cannot be searched");
            }
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            throw new QueryShardException(context, "[" + CONTENT_TYPE + "] field do not support searching, " +
                "use dedicated aggregations instead: ["
                + name() + "]");
        }
    }

    private Explicit<Boolean> ignoreMalformed;

    private Explicit<Set<Metrics>> metrics;

    private AggregateDoubleMetricFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                             Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                             Explicit<Set<Metrics>> metrics,
                                             CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.metrics = metrics;
    }

    @Override
    public AggregateMetricFieldType fieldType() {
        return (AggregateMetricFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType.typeName();
    }

    @Override
    protected AggregateDoubleMetricFieldMapper clone() {
        return (AggregateDoubleMetricFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        Object metricsAsObject;

        if (context.externalValueSet()) {
            metricsAsObject = context.externalValue();
        } else {
            metricsAsObject = context.parser().map();
        }

        if (metricsAsObject == null) {
            metricsAsObject = fieldType().nullValue();
        }

        if (metricsAsObject == null) {
            return;
        }

        EnumMap<Metrics, Object> parsedMetricsMap = new EnumMap<>(Metrics.class);
        if (metricsAsObject instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) metricsAsObject;
            for (Map.Entry<String, Object> e : m.entrySet()) {
                Metrics metric = Metrics.valueOf(e.getKey());

                if (metrics.value().contains(metric) == false) {
                    if (ignoreMalformed.value() == false) {
                        throw new IllegalArgumentException("Aggregate metric [" + metric +
                            "] does not exist in the mapping of field [" + fieldType.name() + "]");
                    } else {
                        context.addIgnoredField(fieldType.name());
                        continue;
                    }
                }

                // Make sure that the value is a number. Probably this will change when
                // new aggregate metric types are added (histogram, cardinality etc)
                if (e.getValue() instanceof Number == false) {
                    if (ignoreMalformed.value() == false) {
                        throw new IllegalArgumentException("Aggregate metric [" + metric +
                            "] of field [" + fieldType.name() + "] must be a number");
                    } else {
                        context.addIgnoredField(fieldType.name());
                        continue;
                    }
                }

                // Make sure that value_count is not a negative value
                if (Metrics.value_count.equals(metric) == true) {
                    if (e.getValue() instanceof Integer == false) {
                        if (ignoreMalformed.value() == false) {
                            throw new IllegalArgumentException("Aggregate metric [" + metric +
                                "] of field [" + fieldType.name() + "] must be an integer number");
                        } else {
                            context.addIgnoredField(fieldType.name());
                            continue;
                        }
                    }

                    Integer metricValue = (Integer) e.getValue();
                    if (metricValue.intValue() < 0) {
                        if (ignoreMalformed.value() == false) {
                            throw new IllegalArgumentException("Aggregate metric [" + metric +
                                "] of field [" + fieldType.name() + "] must not be a negative number");
                        } else {
                            context.addIgnoredField(fieldType.name());
                            continue;
                        }
                    }
                }

                parsedMetricsMap.put(metric, e.getValue());
            }
        } else {
            throw new MapperParsingException("Cannot parse aggregate metric for field [{}]",
                null,fieldType.name());
        }

        // TODO: Implement null_value support?
        if (parsedMetricsMap.keySet().containsAll(metrics.value()) == false
            && ignoreMalformed.value() == false) {
            throw new IllegalArgumentException("Aggregate metric field [" + fieldType.name() +
                "] must contain all metrics " + metrics.value().toString());
        }

        //TODO: Change this and delegate it to NumberType
        for (Map.Entry<Metrics, Object> e : parsedMetricsMap.entrySet()) {
            Double d = null;
            if (e.getValue() instanceof Double) {
                d = (Double) e.getValue();
            } else if (e.getValue() instanceof Integer) {
                d = Double.valueOf((Integer) e.getValue());
            }

            String fieldName = fieldType().name() + "." + e.getKey().name();
            if (fieldType().indexOptions() != IndexOptions.NONE) {
                fields.add(new DoublePoint(fieldName, d));
            }
            if (fieldType().hasDocValues()) {
                fields.add(new SortedNumericDocValuesField(fieldName, Double.doubleToRawLongBits(d)));
            }
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        AggregateDoubleMetricFieldMapper other = (AggregateDoubleMetricFieldMapper) mergeWith;
        if (other.ignoreMalformed.explicit()) {
            this.ignoreMalformed = other.ignoreMalformed;
        }

        if (other.metrics.explicit()) {
            this.metrics = other.metrics;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }

        if (includeDefaults || metrics.explicit()) {
            builder.field(Names.METRICS.getPreferredName(), metrics.value());
        }
    }
}
