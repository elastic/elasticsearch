/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.MultiValueAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.MultiValue;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.xpack.core.spatial.search.aggregations.GeoShapeMetricAggregation;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.utils.OutputFieldNameConverter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt;
import static org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil.isNumericType;

public final class AggregationResultUtils {

    private static final Map<String, AggValueExtractor> TYPE_VALUE_EXTRACTOR_MAP;
    static {
        Map<String, AggValueExtractor> tempMap = new HashMap<>();
        tempMap.put(SingleValue.class.getName(), new SingleValueAggExtractor());
        tempMap.put(ScriptedMetric.class.getName(), new ScriptedMetricAggExtractor());
        tempMap.put(GeoCentroid.class.getName(), new GeoCentroidAggExtractor());
        tempMap.put(GeoBounds.class.getName(), new GeoBoundsAggExtractor());
        tempMap.put(Percentiles.class.getName(), new PercentilesAggExtractor());
        tempMap.put(Range.class.getName(), new RangeAggExtractor());
        tempMap.put(SingleBucketAggregation.class.getName(), new SingleBucketAggExtractor());
        tempMap.put(MultiBucketsAggregation.class.getName(), new MultiBucketsAggExtractor());
        tempMap.put(GeoShapeMetricAggregation.class.getName(), new GeoShapeMetricAggExtractor());
        tempMap.put(MultiValue.class.getName(), new NumericMultiValueAggExtractor());
        tempMap.put(MultiValueAggregation.class.getName(), new MultiValueAggExtractor());
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

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_COORDINATES = "coordinates";
    private static final String POINT = "point";
    private static final String LINESTRING = "linestring";
    private static final String POLYGON = "polygon";

    /**
     * Extracts aggregation results from a composite aggregation and puts it into a map.
     *
     * @param agg The aggregation result
     * @param groups The original groupings used for querying
     * @param aggregationBuilders the aggregation used for querying
     * @param fieldTypeMap A Map containing "field-name": "type" entries to determine the appropriate type for the aggregation results.
     * @param stats stats collector
     * @return a map containing the results of the aggregation in a consumable way
     */
    public static Stream<Map<String, Object>> extractCompositeAggregationResults(
        CompositeAggregation agg,
        GroupConfig groups,
        Collection<AggregationBuilder> aggregationBuilders,
        Collection<PipelineAggregationBuilder> pipelineAggs,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats,
        TransformProgress progress,
        boolean datesAsEpoch
    ) {
        return agg.getBuckets().stream().map(bucket -> {
            stats.incrementNumDocuments(bucket.getDocCount());
            progress.incrementDocsProcessed(bucket.getDocCount());
            progress.incrementDocsIndexed(1L);

            Map<String, Object> document = new HashMap<>();
            // generator to create unique but deterministic document ids, so we
            // - do not create duplicates if we re-run after failure
            // - update documents
            IDGenerator idGen = new IDGenerator();

            groups.getGroups().forEach((destinationFieldName, singleGroupSource) -> {
                Object value = bucket.getKey().get(destinationFieldName);

                idGen.add(destinationFieldName, value);
                updateDocument(
                    document,
                    destinationFieldName,
                    getBucketKeyExtractor(singleGroupSource, datesAsEpoch).value(value, fieldTypeMap.get(destinationFieldName))
                );
            });

            List<String> aggNames = aggregationBuilders.stream().map(AggregationBuilder::getName).collect(Collectors.toList());
            aggNames.addAll(pipelineAggs.stream().map(PipelineAggregationBuilder::getName).collect(Collectors.toList()));

            for (String aggName : aggNames) {
                Aggregation aggResult = bucket.getAggregations().get(aggName);
                // This indicates not that the value contained in the `aggResult` is null, but that the `aggResult` is not
                // present at all in the `bucket.getAggregations`. This could occur in the case of a `bucket_selector` agg, which
                // does not calculate a value, but instead manipulates other results.
                if (aggResult != null) {
                    AggValueExtractor extractor = getExtractor(aggResult);
                    updateDocument(document, aggName, extractor.value(aggResult, fieldTypeMap, ""));
                }
            }

            document.put(TransformField.DOCUMENT_ID_FIELD, idGen.getID());

            return document;
        });
    }

    static BucketKeyExtractor getBucketKeyExtractor(SingleGroupSource groupSource, boolean datesAsEpoch) {
        return BUCKET_KEY_EXTRACTOR_MAP.getOrDefault(
            groupSource.getClass().getName(),
            datesAsEpoch ? DATES_AS_EPOCH_BUCKET_KEY_EXTRACTOR : DEFAULT_BUCKET_KEY_EXTRACTOR
        );
    }

    static AggValueExtractor getExtractor(Aggregation aggregation) {
        if (aggregation instanceof SingleValue) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(SingleValue.class.getName());
        } else if (aggregation instanceof ScriptedMetric) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(ScriptedMetric.class.getName());
        } else if (aggregation instanceof GeoCentroid) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoCentroid.class.getName());
        } else if (aggregation instanceof GeoBounds) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoBounds.class.getName());
            // note: percentiles is also a multi value agg, therefore check percentiles first
            // TODO: can the Percentiles extractor be removed?
        } else if (aggregation instanceof Percentiles) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(Percentiles.class.getName());
            // note: range is also a multi bucket agg, therefore check range first
            // TODO: can the Range extractor be removed?
        } else if (aggregation instanceof Range) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(Range.class.getName());
        } else if (aggregation instanceof MultiValue) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(MultiValue.class.getName());
        } else if (aggregation instanceof MultiValueAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(MultiValueAggregation.class.getName());
        } else if (aggregation instanceof SingleBucketAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(SingleBucketAggregation.class.getName());
        } else if (aggregation instanceof MultiBucketsAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(MultiBucketsAggregation.class.getName());
        } else if (aggregation instanceof GeoShapeMetricAggregation) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoShapeMetricAggregation.class.getName());
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

    @SuppressWarnings("unchecked")
    static void updateDocument(Map<String, Object> document, String fieldName, Object value) {
        String[] fieldTokens = fieldName.split("\\.");
        if (fieldTokens.length == 1) {
            document.put(fieldName, value);
            return;
        }
        Map<String, Object> internalMap = document;
        for (int i = 0; i < fieldTokens.length; i++) {
            String token = fieldTokens[i];
            if (i == fieldTokens.length - 1) {
                if (internalMap.containsKey(token)) {
                    if (internalMap.get(token) instanceof Map) {
                        throw new AggregationExtractionException("mixed object types of nested and non-nested fields [{}]", fieldName);
                    } else {
                        throw new AggregationExtractionException(
                            "duplicate key value pairs key [{}] old value [{}] duplicate value [{}]",
                            fieldName,
                            internalMap.get(token),
                            value
                        );
                    }
                }
                internalMap.put(token, value);
            } else {
                if (internalMap.containsKey(token)) {
                    if (internalMap.get(token) instanceof Map) {
                        internalMap = (Map<String, Object>) internalMap.get(token);
                    } else {
                        throw new AggregationExtractionException("mixed object types of nested and non-nested fields [{}]", fieldName);
                    }
                } else {
                    Map<String, Object> newMap = new HashMap<>();
                    internalMap.put(token, newMap);
                    internalMap = newMap;
                }
            }
        }
    }

    public static class AggregationExtractionException extends ElasticsearchException {
        AggregationExtractionException(String msg, Object... args) {
            super(msg, args);
        }

        AggregationExtractionException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    /**
     * Extract the bucket key and transform it for indexing.
     */
    interface BucketKeyExtractor {

        /**
         * Take the bucket key and return it in the format for the index, taking the mapped type into account.
         *
         * @param key The bucket key for this group source
         * @param type the mapping type of the destination field
         * @return the transformed bucket key for indexing
         */
        Object value(Object key, String type);
    }

    interface AggValueExtractor {
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

    static class MultiValueAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            MultiValueAggregation aggregation = (MultiValueAggregation) agg;
            Map<String, Object> extracted = new HashMap<>();
            for (String valueName : aggregation.valueNames()) {
                List<String> valueAsStrings = aggregation.getValuesAsStrings(valueName);

                // todo: size > 1 is not supported, requires a refactoring so that `size()` is exposed in the agg builder
                if (valueAsStrings.size() > 0) {
                    extracted.put(valueName, valueAsStrings.get(0));
                }
            }

            return extracted;
        }
    }

    static class NumericMultiValueAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            MultiValue aggregation = (MultiValue) agg;
            Map<String, Object> extracted = new HashMap<>();

            String fieldLookupPrefix = (lookupFieldPrefix.isEmpty() ? agg.getName() : lookupFieldPrefix + "." + agg.getName()) + ".";
            for (String valueName : aggregation.valueNames()) {
                double value = aggregation.value(valueName);

                String fieldType = fieldTypeMap.get(fieldLookupPrefix + valueName);
                if (Numbers.isValidDouble(value)) {
                    extracted.put(valueName, dropFloatingPointComponentIfTypeRequiresIt(fieldType, value));
                }
            }

            return extracted;
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

    static class RangeAggExtractor extends MultiBucketsAggExtractor {

        RangeAggExtractor() {
            super(RangeAggExtractor::transformBucketKey);
        }

        private static String transformBucketKey(String bucketKey) {
            return bucketKey.replace(".0-", "-")  // from: convert double to integer
                .replaceAll("\\.0$", "")  // to: convert double to integer
                .replace('.', '_');  // convert remaining dots with underscores so that the key prefix is not treated as object
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

        private final Function<String, String> bucketKeyTransfomer;

        MultiBucketsAggExtractor() {
            this(Function.identity());
        }

        MultiBucketsAggExtractor(Function<String, String> bucketKeyTransfomer) {
            this.bucketKeyTransfomer = Objects.requireNonNull(bucketKeyTransfomer);
        }

        @Override
        public Object value(Aggregation agg, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            MultiBucketsAggregation aggregation = (MultiBucketsAggregation) agg;

            HashMap<String, Object> nested = new HashMap<>();

            for (MultiBucketsAggregation.Bucket bucket : aggregation.getBuckets()) {
                String bucketKey = bucketKeyTransfomer.apply(bucket.getKeyAsString());
                if (bucket.getAggregations().iterator().hasNext() == false) {
                    nested.put(bucketKey, bucket.getDocCount());
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
                    nested.put(bucketKey, nestedBucketObject);
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
                geoShape.put(FIELD_TYPE, POINT);
                geoShape.put(FIELD_COORDINATES, Arrays.asList(aggregation.topLeft().getLon(), aggregation.bottomRight().getLat()));
                // If only the lat or the lon of the two geo_points are equal, than we know it should be a line
            } else if (Double.compare(aggregation.topLeft().getLat(), aggregation.bottomRight().getLat()) == 0
                || Double.compare(aggregation.topLeft().getLon(), aggregation.bottomRight().getLon()) == 0) {
                    geoShape.put(FIELD_TYPE, LINESTRING);
                    geoShape.put(
                        FIELD_COORDINATES,
                        Arrays.asList(
                            new Double[] { aggregation.topLeft().getLon(), aggregation.topLeft().getLat() },
                            new Double[] { aggregation.bottomRight().getLon(), aggregation.bottomRight().getLat() }
                        )
                    );
                } else {
                    // neither points are equal, we have a polygon that is a square
                    geoShape.put(FIELD_TYPE, POLYGON);
                    final GeoPoint tl = aggregation.topLeft();
                    final GeoPoint br = aggregation.bottomRight();
                    geoShape.put(
                        FIELD_COORDINATES,
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

    static class GeoShapeMetricAggExtractor implements AggValueExtractor {

        @Override
        public Object value(Aggregation aggregation, Map<String, String> fieldTypeMap, String lookupFieldPrefix) {
            assert aggregation instanceof GeoShapeMetricAggregation
                : "Unexpected type [" + aggregation.getClass().getName() + "] for aggregation [" + aggregation.getName() + "]";
            return ((GeoShapeMetricAggregation) aggregation).geoJSONGeometry();
        }
    }

    static class GeoTileBucketKeyExtractor implements BucketKeyExtractor {

        @Override
        public Object value(Object key, String type) {
            assert key instanceof String;
            Rectangle rectangle = GeoTileUtils.toBoundingBox(key.toString());
            final Map<String, Object> geoShape = new HashMap<>();
            geoShape.put(FIELD_TYPE, POLYGON);
            geoShape.put(
                FIELD_COORDINATES,
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
