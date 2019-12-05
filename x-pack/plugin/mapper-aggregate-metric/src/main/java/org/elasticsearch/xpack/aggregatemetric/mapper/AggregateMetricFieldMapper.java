/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;


import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link FieldMapper} for a field containing aggregate metrics such as min/max/value_count etc. */
public class AggregateMetricFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "aggregate_metric";

    /**
     * Mapping field names
     */
    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
        public static final ParseField METRICS = new ParseField("metrics");
    }

    enum Metrics {
        min, max, sum, value_count;
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Set<String>> METRICS = new Explicit<>(Collections.emptySet(), false);
        public static final AggregateMetricFieldType FIELD_TYPE = new AggregateMetricFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<AggregateMetricFieldMapper.Builder, AggregateMetricFieldMapper> {

        private Boolean ignoreMalformed;

        private Set<String> metrics;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public AggregateMetricFieldMapper.Builder ignoreMalformed(boolean ignoreMalformed) {
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
            return AggregateMetricFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        public AggregateMetricFieldMapper.Builder metrics(Set<String> metrics) {
            this.metrics = metrics;
            return builder;
        }

        protected Explicit<Set<String>> metrics(BuilderContext context) {
            if (metrics != null) {
                return new Explicit<>(metrics, true);
            }
            return Defaults.METRICS;
        }

        @Override
        public AggregateMetricFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new AggregateMetricFieldMapper(name, fieldType, defaultFieldType,
                context.indexSettings(), multiFieldsBuilder.build(this, context),
                ignoreMalformed(context), metrics(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<Builder, AggregateMetricFieldMapper> parse(String name,
                                                                         Map<String, Object> node,
                                                                         ParserContext parserContext) throws MapperParsingException {
            AggregateMetricFieldMapper.Builder builder = new AggregateMetricFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals(Names.METRICS.getPreferredName())) {
                    String metricsStr[] = XContentMapValues.nodeStringArrayValue(propNode);
                    // Convert the array of Metric enum to a Set of their string representation
                    Set<String> supporteddMetrics = Arrays.stream(Metrics.values()).map(s -> s.toString()).collect(Collectors.toSet());

                    // Make sure that metrics are supported
                    Set<String> parsedMetrics = new LinkedHashSet<>();
                    for (int i = 0; i < metricsStr.length; i++) {
                        if (supporteddMetrics.contains(metricsStr[i]) == false) {
                            throw new MapperParsingException("Metric [" + metricsStr[i] + "] is not supported.");
                        } else {
                            parsedMetrics.add(metricsStr[i]);
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

    private Explicit<Set<String>> metrics;

    private AggregateMetricFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
        Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed, Explicit<Set<String>> metrics,
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
    protected AggregateMetricFieldMapper clone() {
        return (AggregateMetricFieldMapper) super.clone();
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

        Map<String, Object> parsedMetricsMap = new LinkedHashMap<>();
        if (metricsAsObject instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) metricsAsObject;
            for (Map.Entry<String, Object> e : m.entrySet()) {
                String metricName = e.getKey();

                if (metrics.value().contains(metricName) == false) {
                    if (ignoreMalformed.value() == false) {
                        throw new IllegalArgumentException("Aggregate metric [" + metricName +
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
                        throw new IllegalArgumentException("Aggregate metric [" + metricName +
                            "] of field [" + fieldType.name() + "] must be a number");
                    } else {
                        context.addIgnoredField(fieldType.name());
                        continue;
                    }
                }

                // Make sure that value_count is not a negative value
                if (Metrics.value_count.toString().equals(metricName) == true) {
                    if (e.getValue() instanceof Integer == false) {
                        if (ignoreMalformed.value() == false) {
                            throw new IllegalArgumentException("Aggregate metric [" + metricName +
                                "] of field [" + fieldType.name() + "] must be an integer number");
                        } else {
                            context.addIgnoredField(fieldType.name());
                            continue;
                        }
                    }

                    Integer metricValue = (Integer) e.getValue();
                    if (metricValue.intValue() < 0) {
                        if (ignoreMalformed.value() == false) {
                            throw new IllegalArgumentException("Aggregate metric [" + metricName +
                                "] of field [" + fieldType.name() + "] must not be a negative number");
                        } else {
                            context.addIgnoredField(fieldType.name());
                            continue;
                        }
                    }
                }

                parsedMetricsMap.put(metricName, e.getValue());
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

        if (fieldType().hasDocValues()) {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.writeMap(parsedMetricsMap);
            BytesRef b = streamOutput.bytes().toBytesRef();
            fields.add(new BinaryDocValuesField(simpleName(), b));

            streamOutput.close();
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        AggregateMetricFieldMapper other = (AggregateMetricFieldMapper) mergeWith;
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
