/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.utils.OutputFieldNameConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt;
import static org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil.isNumericType;

/**
 * A utility for extract aggregations into a format that can be indexed
 */
public final class AggregationValueExtractor {
    private static final Map<String, AggValueExtractor> TYPE_VALUE_EXTRACTOR_MAP;
    static {
        Map<String, AggValueExtractor> tempMap = new HashMap<>();
        tempMap.put(NumericMetricsAggregation.SingleValue.class.getName(), new SingleValueAggExtractor());
        tempMap.put(ScriptedMetric.class.getName(), new ScriptedMetricAggExtractor());
        tempMap.put(GeoCentroid.class.getName(), new GeoCentroidAggExtractor());
        tempMap.put(GeoBounds.class.getName(), new GeoBoundsAggExtractor());
        tempMap.put(Percentiles.class.getName(), new PercentilesAggExtractor());
        tempMap.put(SingleBucketAggregation.class.getName(), new SingleBucketAggExtractor());
        tempMap.put(MultiBucketsAggregation.class.getName(), new MultiBucketsAggExtractor());
        TYPE_VALUE_EXTRACTOR_MAP = Collections.unmodifiableMap(tempMap);
    }

    private static final Map<String, BucketKeyExtractor> BUCKET_KEY_EXTRACTOR_MAP;
    private static final BucketKeyExtractor DEFAULT_BUCKET_KEY_EXTRACTOR = new DefaultBucketKeyExtractor();
    private static final BucketKeyExtractor DATES_AS_EPOCH_BUCKET_KEY_EXTRACTOR = new DatesAsEpochBucketKeyExtractor();

    static {
        Map<String, BucketKeyExtractor> tempMap = new HashMap<>();
        tempMap.put(GeoTileGroupSource.class.getName(), new GeoTileBucketKeyExtractor());

        BUCKET_KEY_EXTRACTOR_MAP = Collections.unmodifiableMap(tempMap);
    }

    /**
     * Constructs a valid extractor for getting bucket key values into a value that can be indexed.
     * @param groupSource The groupSource from which to construct an aggregation value extractor
     * @param datesAsEpoch Whether or not dates should be epoch milliseconds or not
     * @return An extractor for bucket key values
     */
    public static BucketKeyExtractor getBucketKeyExtractor(SingleGroupSource groupSource, boolean datesAsEpoch) {
        return BUCKET_KEY_EXTRACTOR_MAP.getOrDefault(
            groupSource.getClass().getName(),
            datesAsEpoch ? DATES_AS_EPOCH_BUCKET_KEY_EXTRACTOR : DEFAULT_BUCKET_KEY_EXTRACTOR
        );
    }

    /**
     * Constructs a {@link AggValueExtractor} so that aggregation values can be extracted into an indexable format.
     * @param aggregation Get a value extractor for the given aggregation
     * @return The appropriate AggValueExtractor
     */
    public static AggValueExtractor getExtractor(Aggregation aggregation) {
        if (aggregation instanceof SingleValue) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(SingleValue.class.getName());
        } else if (aggregation instanceof ScriptedMetric) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(ScriptedMetric.class.getName());
        } else if (aggregation instanceof GeoCentroid) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoCentroid.class.getName());
        } else if (aggregation instanceof GeoBounds) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoBounds.class.getName());
        } else if (aggregation instanceof Percentiles) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(Percentiles.class.getName());
        } else if (aggregation instanceof SingleBucketAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(SingleBucketAggregation.class.getName());
        } else if (aggregation instanceof MultiBucketsAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(MultiBucketsAggregation.class.getName());
        } else {
            // Execution should never reach this point!
            // Creating transforms with unsupported aggregations shall not be possible
            throw new AggregationExtractionException(
                "unsupported aggregation [{}] with name [{}]",
                aggregation.getType(),
                aggregation.getName()
            );
        }
    }

    /**
     * This gets thrown when there is some issue constructing or finding a valid aggregation value extractor
     */
    public static class AggregationExtractionException extends ElasticsearchException {
        public AggregationExtractionException(String msg, Object... args) {
            super(msg, args);
        }
    }

    /**
     * Extract the bucket key and transform it for indexing.
     */
    public interface BucketKeyExtractor {

        /**
         * Take the bucket key and return it in the format for the index, taking the mapped type into account.
         *
         * @param key The bucket key for this group source
         * @param type the mapping type of the destination field
         * @return the transformed bucket key for indexing
         */
        Object value(Object key, String type);
    }

    /**
     * Extract the aggregation value ane transform it for indexing
     */
    public interface AggValueExtractor {
        /**
         * The value ready for indexing. This takes the mapped type into account
         * @param aggregation The aggregation whose value needs extracted and transformed
         * @param fieldTypeMap A map from field name to mapped type
         * @return the transformed aggregation value for indexing
         */
        default Object value(Aggregation aggregation, Map<String, String> fieldTypeMap) {
            return value(aggregation, fieldTypeMap, "");
        }

        Object value(Aggregation aggregation, Map<String, String> fieldTypeMap, String lookupFieldPrefix);
    }

    static class SingleValueAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            SingleValue aggregation = (SingleValue) agg;
            // If the double is invalid, this indicates sparse data
            if (Numbers.isValidDouble(aggregation.value()) == false) {
                return null;
            }

            String fieldType = fieldTypeMap.get(lookupFieldPrefix.isEmpty() ? agg.getName() : lookupFieldPrefix + "." + agg.getName());
            // If the type is numeric or if the formatted string is the same as simply making the value a string,
            // gather the `value` type, otherwise utilize `getValueAsString` so we don't lose formatted outputs.
            if (isNumericType(fieldType) || aggregation.getValueAsString().equals(String.valueOf(aggregation.value()))) {
                return dropFloatingPointComponentIfTypeRequiresIt(fieldType, aggregation.value());
            } else {
                return aggregation.getValueAsString();
            }
        }
    }

    static class PercentilesAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            Percentiles aggregation = (Percentiles) agg;
            HashMap<String, Double> percentiles = new HashMap<>();

            for (Percentile p : aggregation) {
                // in case of sparse data percentiles might not have data, in this case it returns NaN,
                // we need to guard the output and set null in this case
                if (Numbers.isValidDouble(p.getValue()) == false) {
                    percentiles.put(OutputFieldNameConverter.fromDouble(p.getPercent()), null);
                } else {
                    percentiles.put(OutputFieldNameConverter.fromDouble(p.getPercent()), p.getValue());
                }
            }

            return percentiles;
        }
    }

    static class SingleBucketAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            SingleBucketAggregation aggregation = (SingleBucketAggregation) agg;

            if (aggregation.getAggregations().iterator().hasNext() == false) {
                return aggregation.getDocCount();
            }

            HashMap<String, Object> nested = new HashMap<>();
            for (Aggregation subAgg : aggregation.getAggregations()) {
                nested.put(
                    subAgg.getName(),
                    getExtractor(subAgg).value(
                        subAgg,
                        fieldTypeMap,
                        lookupFieldPrefix.isEmpty() ? agg.getName() : lookupFieldPrefix + "." + agg.getName()
                    )
                );
            }

            return nested;
        }
    }

    static class MultiBucketsAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            MultiBucketsAggregation aggregation = (MultiBucketsAggregation) agg;

            HashMap<String, Object> nested = new HashMap<>();

            for (MultiBucketsAggregation.Bucket bucket : aggregation.getBuckets()) {
                if (bucket.getAggregations().iterator().hasNext() == false) {
                    nested.put(bucket.getKeyAsString(), bucket.getDocCount());
                } else {
                    HashMap<String, Object> nestedBucketObject = new HashMap<>();
                    for (Aggregation subAgg : bucket.getAggregations()) {
                        nestedBucketObject.put(
                            subAgg.getName(),
                            getExtractor(subAgg).value(
                                subAgg,
                                fieldTypeMap,
                                lookupFieldPrefix.isEmpty() ? agg.getName() : lookupFieldPrefix + "." + agg.getName()
                            )
                        );
                    }
                    nested.put(bucket.getKeyAsString(), nestedBucketObject);
                }
            }
            return nested;
        }
    }

    static class ScriptedMetricAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            ScriptedMetric aggregation = (ScriptedMetric) agg;
            return aggregation.aggregation();
        }
    }

    static class GeoCentroidAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            GeoCentroid aggregation = (GeoCentroid) agg;
            // if the account is `0` iff there is no contained centroid
            return aggregation.count() > 0 ? aggregation.centroid().toString() : null;
        }
    }

    static class GeoBoundsAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            GeoBounds aggregation = (GeoBounds) agg;
            if (aggregation.bottomRight() == null || aggregation.topLeft() == null) {
                return null;
            }
            final Map<String, Object> geoShape = new HashMap<>();
            // If the two geo_points are equal, it is a point
            if (aggregation.topLeft().equals(aggregation.bottomRight())) {
                geoShape.put(ShapeParser.FIELD_TYPE.getPreferredName(), PointBuilder.TYPE.shapeName());
                geoShape.put(
                    ShapeParser.FIELD_COORDINATES.getPreferredName(),
                    Arrays.asList(aggregation.topLeft().getLon(), aggregation.bottomRight().getLat())
                );
                // If only the lat or the lon of the two geo_points are equal, than we know it should be a line
            } else if (Double.compare(aggregation.topLeft().getLat(), aggregation.bottomRight().getLat()) == 0
                || Double.compare(aggregation.topLeft().getLon(), aggregation.bottomRight().getLon()) == 0) {
                geoShape.put(ShapeParser.FIELD_TYPE.getPreferredName(), LineStringBuilder.TYPE.shapeName());
                geoShape.put(
                    ShapeParser.FIELD_COORDINATES.getPreferredName(),
                    Arrays.asList(
                        new Double[] { aggregation.topLeft().getLon(), aggregation.topLeft().getLat() },
                        new Double[] { aggregation.bottomRight().getLon(), aggregation.bottomRight().getLat() }
                    )
                );
            } else {
                // neither points are equal, we have a polygon that is a square
                geoShape.put(ShapeParser.FIELD_TYPE.getPreferredName(), PolygonBuilder.TYPE.shapeName());
                final GeoPoint tl = aggregation.topLeft();
                final GeoPoint br = aggregation.bottomRight();
                geoShape.put(
                    ShapeParser.FIELD_COORDINATES.getPreferredName(),
                    Collections.singletonList(
                        Arrays.asList(
                            new Double[] { tl.getLon(), tl.getLat() },
                            new Double[] { br.getLon(), tl.getLat() },
                            new Double[] { br.getLon(), br.getLat() },
                            new Double[] { tl.getLon(), br.getLat() },
                            new Double[] { tl.getLon(), tl.getLat() }
                        )
                    )
                );
            }
            return geoShape;
        }
    }

    static class GeoTileBucketKeyExtractor implements BucketKeyExtractor {

        @Override
        public Object value(Object key, String type) {
            assert key instanceof String;
            Rectangle rectangle = GeoTileUtils.toBoundingBox(key.toString());
            final Map<String, Object> geoShape = new HashMap<>();
            geoShape.put(ShapeParser.FIELD_TYPE.getPreferredName(), PolygonBuilder.TYPE.shapeName());
            geoShape.put(
                ShapeParser.FIELD_COORDINATES.getPreferredName(),
                Collections.singletonList(
                    Arrays.asList(
                        new Double[] { rectangle.getMaxLon(), rectangle.getMinLat() },
                        new Double[] { rectangle.getMinLon(), rectangle.getMinLat() },
                        new Double[] { rectangle.getMinLon(), rectangle.getMaxLat() },
                        new Double[] { rectangle.getMaxLon(), rectangle.getMaxLat() },
                        new Double[] { rectangle.getMaxLon(), rectangle.getMinLat() }
                    )
                )
            );
            return geoShape;
        }

    }

    static class DefaultBucketKeyExtractor implements BucketKeyExtractor {

        @Override
        public Object value(Object key, String type) {
            if (isNumericType(type) && key instanceof Double) {
                return dropFloatingPointComponentIfTypeRequiresIt(type, (Double) key);
            } else if ((DateFieldMapper.CONTENT_TYPE.equals(type) || DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(type))
                && key instanceof Long) {
                // date_histogram return bucket keys with milliseconds since epoch precision, therefore we don't need a
                // nanosecond formatter, for the parser on indexing side, time is optional (only the date part is mandatory)
                return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis((Long) key);
            }

            return key;
        }

    }

    static class DatesAsEpochBucketKeyExtractor implements BucketKeyExtractor {

        @Override
        public Object value(Object key, String type) {
            if (isNumericType(type) && key instanceof Double) {
                return dropFloatingPointComponentIfTypeRequiresIt(type, (Double) key);
            }
            return key;
        }

    }
}
