/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.utils.OutputFieldNameConverter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        tempMap.put(SingleBucketAggregation.class.getName(), new SingleBucketAggExtractor());
        tempMap.put(MultiBucketsAggregation.class.getName(), new MultiBucketsAggExtractor());
        TYPE_VALUE_EXTRACTOR_MAP = Collections.unmodifiableMap(tempMap);
    }

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
        TransformIndexerStats stats
    ) {
        return agg.getBuckets().stream().map(bucket -> {
            stats.incrementNumDocuments(bucket.getDocCount());
            Map<String, Object> document = new HashMap<>();
            // generator to create unique but deterministic document ids, so we
            // - do not create duplicates if we re-run after failure
            // - update documents
            IDGenerator idGen = new IDGenerator();

            groups.getGroups().forEach((destinationFieldName, singleGroupSource) -> {
                Object value = bucket.getKey().get(destinationFieldName);
                idGen.add(destinationFieldName, value);
                updateDocument(document, destinationFieldName, singleGroupSource.transformBucketKey(value));
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

    static AggValueExtractor getExtractor(Aggregation aggregation) {
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
                return aggregation.value();
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

}
