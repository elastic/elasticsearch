/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesFieldsTests extends MapperServiceTestCase {

    public void testTimeSeriesFields() throws IOException {
        String mappings = """
            {
                "_doc": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "metrics": {
                            "properties": {
                                "counter": {
                                    "type": "short",
                                    "time_series_metric": "counter"
                                },
                                "gauge": {
                                    "type": "double",
                                    "time_series_metric": "gauge"
                                }
                            }
                        },
                        "dimension": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "dimension_with_subfields": {
                            "type": "keyword",
                            "time_series_dimension": true,
                            "fields": {
                                "keyword": {
                                    "type":"keyword"
                                }
                            }
                        },
                        "label_with_dimension": {
                            "type": "keyword",
                            "fields": {
                                "keyword": {
                                    "type":"keyword",
                                    "time_series_dimension": true
                                }
                            }
                        },
                        "no_label": {
                            "type": "text"
                        },
                        "just_a_label": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type":"keyword"
                                }
                            }
                        },
                        "metric_with_subfields": {
                            "type": "double",
                            "time_series_metric": "gauge",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                }
            }""";
        TimeSeriesFields timeSeriesFields = getTimeSeriesFields(mappings);
        assertThat(timeSeriesFields.metricFields(), arrayContainingInAnyOrder("metrics.counter", "metrics.gauge", "metric_with_subfields"));
        assertThat(
            timeSeriesFields.dimensionFields(),
            arrayContainingInAnyOrder("dimension", "dimension_with_subfields", "label_with_dimension")
        );
        assertThat(timeSeriesFields.labelFields(), arrayContainingInAnyOrder("just_a_label"));
        assertThat(
            timeSeriesFields.multiFieldSources(),
            equalTo(Map.of("just_a_label", "just_a_label.keyword", "label_with_dimension", "label_with_dimension.keyword"))
        );
    }

    public void testTimeSeriesFieldsWithFlattenedDimensions() throws IOException {
        String mappings = """
            {
                "_doc": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "metrics": {
                            "properties": {
                                "counter": {
                                    "type": "short",
                                    "time_series_metric": "counter"
                                },
                                "gauge": {
                                    "type": "double",
                                    "time_series_metric": "gauge"
                                }
                            }
                        },
                        "dimension": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "flattened_dimension": {
                            "type": "flattened",
                            "time_series_dimensions": ["d_1", "d_2"]
                        },
                        "flattened_label": {
                            "type": "flattened"
                        }
                    }
                }
            }""";
        TimeSeriesFields timeSeriesFields = getTimeSeriesFields(mappings);
        assertThat(timeSeriesFields.metricFields(), arrayContainingInAnyOrder("metrics.counter", "metrics.gauge"));
        assertThat(
            timeSeriesFields.dimensionFields(),
            arrayContainingInAnyOrder("dimension", "flattened_dimension.d_1", "flattened_dimension.d_2")
        );
        assertThat(timeSeriesFields.labelFields(), arrayContainingInAnyOrder("flattened_label"));
        assertThat(timeSeriesFields.multiFieldSources().isEmpty(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    private TimeSeriesFields getTimeSeriesFields(String mappings) throws IOException {
        MapperService mapperService = createMapperService(mappings);
        Map<String, Object> map = XContentHelper.convertToMap(
            new CompressedXContent(mappings).compressedReference(),
            true,
            XContentType.JSON
        ).v2();
        return new TimeSeriesFields.Collector(mapperService, "@timestamp").collect((Map<String, ?>) map.get("_doc"));
    }
}
