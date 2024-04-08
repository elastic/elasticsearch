/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@link CoreValuesSourceType} holds the {@link ValuesSourceType} implementations for the core aggregations package.
 */
public enum CoreValuesSourceType implements ValuesSourceType {

    NUMERIC() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Numeric.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {

            if ((fieldContext.indexFieldData() instanceof IndexNumericFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected numeric type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }

            ValuesSource.Numeric dataSource = new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData());
            if (script != null) {
                // Value script case
                dataSource = new ValuesSource.Numeric.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            Number missing;
            if (rawMissing instanceof Number) {
                missing = (Number) rawMissing;
            } else {
                missing = docValueFormat.parseDouble(rawMissing.toString(), false, nowInMillis);
            }
            return MissingValues.replaceMissing((ValuesSource.Numeric) valuesSource, missing);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            /* TODO: this silently ignores a timezone argument, whereas NumberFieldType#docValueFormat throws if given a time zone.
                     Before we can solve this, we need to resolve https://github.com/elastic/elasticsearch/issues/47469 which deals
                     with the fact that the same formatter is used for input and output values.  We want to support a use case in SQL
                     (and elsewhere) that allows for passing a long value milliseconds since epoch into date aggregations.  In that case,
                     the timezone is sensible as part of the bucket key format.
             */
            if (format == null) {
                return DocValueFormat.RAW;
            } else {
                return new DocValueFormat.Decimal(format);
            }

        }
    },
    KEYWORD() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Bytes.WithOrdinals.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Bytes.Script(script);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();
            ValuesSource dataSource;
            if (indexFieldData instanceof IndexOrdinalsFieldData) {
                dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData((IndexOrdinalsFieldData) indexFieldData);
            } else {
                dataSource = new ValuesSource.Bytes.FieldData(indexFieldData);
            }
            if (script != null) {
                // Again, what's the difference between WithScript and Script?
                dataSource = new ValuesSource.Bytes.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            final BytesRef missing = docValueFormat.parseBytesRef(rawMissing.toString());
            if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                return MissingValues.replaceMissing((ValuesSource.Bytes.WithOrdinals) valuesSource, missing);
            } else {
                return MissingValues.replaceMissing((ValuesSource.Bytes) valuesSource, missing);
            }
        }
    },
    GEOPOINT() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.GeoPoint.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw AggregationErrors.valuesSourceDoesNotSupportScritps(this.value());
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (fieldContext.indexFieldData() instanceof IndexGeoPointFieldData pointFieldData) {
                return new ValuesSource.GeoPoint.Fielddata(pointFieldData);
            }
            throw new IllegalArgumentException(
                "Expected geo_point type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
            );
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            // TODO: also support the structured formats of geo points
            final GeoPoint missing = new GeoPoint(rawMissing.toString());
            return MissingValues.replaceMissing((ValuesSource.GeoPoint) valuesSource, missing);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.GEOHASH;
        }
    },
    RANGE() {
        @Override
        public ValuesSource getEmpty() {
            throw new IllegalArgumentException("Can't deal with unmapped ValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw AggregationErrors.valuesSourceDoesNotSupportScritps(this.value());
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            MappedFieldType fieldType = fieldContext.fieldType();

            if (fieldType instanceof RangeFieldMapper.RangeFieldType == false) {
                throw new IllegalArgumentException("Asked for range ValuesSource, but field is of type " + fieldType.name());
            }
            RangeFieldMapper.RangeFieldType rangeFieldType = (RangeFieldMapper.RangeFieldType) fieldType;
            return new ValuesSource.Range(fieldContext.indexFieldData(), rangeFieldType.rangeType());
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    },
    IP() {
        @Override
        public ValuesSource getEmpty() {
            return KEYWORD.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return KEYWORD.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return KEYWORD.getField(fieldContext, script);
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            return KEYWORD.replaceMissing(valuesSource, rawMissing, docValueFormat, nowInMillis);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.IP;
        }
    },
    DATE() {
        @Override
        public ValuesSource getEmpty() {
            return NUMERIC.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return NUMERIC.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            ValuesSource.Numeric dataSource = fieldData(fieldContext);
            if (script != null) {
                // Value script case
                return new ValuesSource.Numeric.WithScript(dataSource, script);
            }
            return dataSource;
        }

        private ValuesSource.Numeric fieldData(FieldContext fieldContext) {
            if ((fieldContext.indexFieldData() instanceof IndexNumericFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected numeric type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }
            if (fieldContext.fieldType() instanceof DateFieldType == false) {
                return new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData());
            }

            return new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData()) {
                /**
                 * Proper dates get a real implementation of
                 * {@link #roundingPreparer(AggregationContext)}. If the field is
                 * configured with a script or a missing value then we'll
                 * wrap this without delegating so those fields will ignore
                 * this implementation. Which is correct.
                 */
                @Override
                public Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) throws IOException {
                    DateFieldType dft = (DateFieldType) fieldContext.fieldType();
                    /*
                     * The range of dates, min first, then max. This is an array so we can
                     * write to it inside the QueryVisitor below.
                     */
                    long[] range = new long[] { Long.MIN_VALUE, Long.MAX_VALUE };

                    // Check the search index for bounds
                    if (fieldContext.fieldType().isIndexed()) {
                        log.trace("Attempting to apply index bound date rounding");
                        /*
                         * We can't look up the min and max date without both the
                         * search index (isSearchable) and the resolution which
                         * is on the DateFieldType.
                         */
                        byte[] min = PointValues.getMinPackedValue(context.searcher().getIndexReader(), fieldContext.field());
                        if (min != null) {
                            // null means that there aren't values in the index
                            byte[] max = PointValues.getMaxPackedValue(context.searcher().getIndexReader(), fieldContext.field());
                            range[0] = dft.resolution().parsePointAsMillis(min);
                            range[1] = dft.resolution().parsePointAsMillis(max);
                        }
                    }
                    log.trace("Bounds after index bound date rounding: {}, {}", range[0], range[1]);

                    boolean isMultiValue = false;
                    for (LeafReaderContext leaf : context.searcher().getLeafContexts()) {
                        if (fieldContext.fieldType().isIndexed()) {
                            PointValues pointValues = leaf.reader().getPointValues(fieldContext.field());
                            if (pointValues != null && pointValues.size() != pointValues.getDocCount()) {
                                isMultiValue = true;
                            }
                        } else if (fieldContext.fieldType().hasDocValues()) {
                            if (DocValues.unwrapSingleton(leaf.reader().getSortedNumericDocValues(fieldContext.field())) == null) {
                                isMultiValue = true;
                            }
                        }
                    }

                    // Check the query for bounds. If the field is multivalued, we can't apply query bounds, because a document that
                    // matches the query might also have values outside the query, which would not be included in any range.
                    if (context.query() != null && false == isMultiValue) {
                        log.trace("Attempting to apply query bound rounding");
                        context.query().visit(new QueryVisitor() {
                            @Override
                            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                                // Only extract bounds queries that must filter the results
                                return switch (occur) {
                                    case MUST, FILTER -> this;
                                    default -> QueryVisitor.EMPTY_VISITOR;
                                };
                            };

                            @Override
                            public boolean acceptField(String field) {
                                return field.equals(fieldContext.fieldType().name());
                            };

                            @Override
                            public void visitLeaf(Query query) {
                                if (query instanceof PointRangeQuery prq) {
                                    range[0] = Math.max(range[0], dft.resolution().parsePointAsMillis(prq.getLowerPoint()));
                                    range[1] = Math.min(range[1], dft.resolution().parsePointAsMillis(prq.getUpperPoint()));
                                }
                            };
                        });
                    }
                    log.trace("Bounds after query bound date rounding: {}, {}", range[0], range[1]);

                    if (range[0] == Long.MIN_VALUE && range[1] == Long.MAX_VALUE) {
                        // Didn't find any bounds
                        log.trace("Unable to find rounding bounds");
                        return Rounding::prepareForUnknown;
                    }

                    // If we have bounds stepping over each other from query bound checks, return an unknown rounding
                    // (which we expect to never actually get any values to round).
                    if (range[0] > range[1]) {
                        return Rounding::prepareForUnknown;
                    }

                    return rounding -> rounding.prepare(range[0], range[1]);
                }
            };
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, nowInMillis);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return new DocValueFormat.DateTime(
                format == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER : DateFormatter.forPattern(format),
                tz == null ? ZoneOffset.UTC : tz,
                // If we were just looking at fields, we could read the resolution from the field settings, but we need to deal with script
                // output, which has no way to indicate the resolution, so we need to default to something. Milliseconds is the standard.
                DateFieldMapper.Resolution.MILLISECONDS
            );
        }
    },
    BOOLEAN() {
        @Override
        public ValuesSource getEmpty() {
            return NUMERIC.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return NUMERIC.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return NUMERIC.getField(fieldContext, script);
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, nowInMillis);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.BOOLEAN;
        }
    };

    public static final Logger log = LogManager.getLogger(CoreValuesSourceType.class);

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String typeName() {
        return value();
    }

    /** List containing all members of the enumeration. */
    public static final List<ValuesSourceType> ALL_CORE = Arrays.asList(CoreValuesSourceType.values());
}
