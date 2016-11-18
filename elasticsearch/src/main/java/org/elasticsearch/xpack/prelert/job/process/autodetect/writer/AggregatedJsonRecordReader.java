/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;

/**
 * Reads highly hierarchical JSON structures.  Currently very much geared to the
 * outputIndex of Elasticsearch's aggregations.  Could be made more generic in the
 * future if another slightly different hierarchical JSON structure needs to be
 * parsed.
 */
class AggregatedJsonRecordReader extends AbstractJsonRecordReader {
    private static final String AGG_KEY = "key";
    private static final String AGG_VALUE = "value";

    private boolean isFirstTime = true;

    private final List<String> nestingOrder;
    private List<String> nestedValues;
    private String latestDocCount;

    /**
     * Create a reader that simulates simple records given a hierarchical JSON
     * structure where each field is at a progressively deeper level of nesting.
     */
    AggregatedJsonRecordReader(JsonParser parser, Map<String, Integer> fieldMap, String recordHoldingField, Logger logger,
            List<String> nestingOrder) {
        super(parser, fieldMap, recordHoldingField, logger);
        this.nestingOrder = Objects.requireNonNull(nestingOrder);
        if (this.nestingOrder.isEmpty()) {
            throw new IllegalArgumentException(
                    "Expected nesting order for aggregated JSON must not be empty");
        }
        nestedValues = new ArrayList<>();
    }

    /**
     * Read forwards in the JSON until enough information has been gathered to
     * write to the record array.
     *
     * @param record    Read fields are written to this array. This array is first filled with empty
     *                  strings and will never contain a <code>null</code>
     * @param gotFields boolean array each element is true if that field
     *                  was read
     * @return The number of fields in the aggregated hierarchy, or -1 if nothing was read
     * because the end of the stream was reached
     */
    @Override
    public long read(String[] record, boolean[] gotFields) throws IOException {
        initArrays(record, gotFields);
        latestDocCount = null;
        fieldCount = 0;
        if (isFirstTime) {
            clearNestedLevel();
            consumeToRecordHoldingField();
            isFirstTime = false;
        }

        boolean gotInnerValue = false;
        JsonToken token = tryNextTokenOrReadToEndOnError();
        while (!(token == JsonToken.END_OBJECT && nestedLevel == 0)) {
            if (token == null) {
                break;
            }

            if (token == JsonToken.START_OBJECT) {
                ++nestedLevel;
            } else if (token == JsonToken.END_OBJECT) {
                if (gotInnerValue) {
                    completeRecord(record, gotFields);
                }
                --nestedLevel;
                if (nestedLevel % 2 == 0 && !nestedValues.isEmpty()) {
                    nestedValues.remove(nestedValues.size() - 1);
                }
                if (gotInnerValue) {
                    break;
                }
            } else if (token == JsonToken.FIELD_NAME) {
                if (((nestedLevel + 1) / 2) == nestingOrder.size()) {
                    gotInnerValue = parseFieldValuePair(record, gotFields) || gotInnerValue;
                }
                // Alternate nesting levels are arbitrary labels that can be
                // ignored.
                else if (nestedLevel > 0 && nestedLevel % 2 == 0) {
                    String fieldName = parser.getCurrentName();
                    if (fieldName.equals(AGG_KEY)) {
                        token = tryNextTokenOrReadToEndOnError();
                        if (token == null) {
                            break;
                        }
                        nestedValues.add(parser.getText());
                    } else if (fieldName.equals(SchedulerConfig.DOC_COUNT)) {
                        token = tryNextTokenOrReadToEndOnError();
                        if (token == null) {
                            break;
                        }
                        latestDocCount = parser.getText();
                    }
                }
            }

            token = tryNextTokenOrReadToEndOnError();
        }

        // null token means EOF; nestedLevel 0 means we've reached the end of
        // the aggregations object
        if (token == null || nestedLevel == 0) {
            return -1;
        }
        return fieldCount;
    }

    @Override
    protected void clearNestedLevel() {
        nestedLevel = 0;
    }

    private boolean parseFieldValuePair(String[] record, boolean[] gotFields) throws IOException {
        String fieldName = parser.getCurrentName();
        JsonToken token = tryNextTokenOrReadToEndOnError();

        if (token == null) {
            return false;
        }

        if (token == JsonToken.START_OBJECT) {
            ++nestedLevel;
            return false;
        }

        if (token == JsonToken.START_ARRAY) {
            // We don't expect arrays at this level of aggregated inputIndex
            // (although we do expect arrays at higher levels).  Consume the
            // whole array but do nothing with it.
            while (token != JsonToken.END_ARRAY) {
                token = tryNextTokenOrReadToEndOnError();
            }
            return false;
        }

        ++fieldCount;

        if (AGG_VALUE.equals(fieldName)) {
            fieldName = nestingOrder.get(nestingOrder.size() - 1);
        }

        Integer index = fieldMap.get(fieldName);
        if (index == null) {
            return false;
        }

        String fieldValue = parser.getText();
        record[index] = fieldValue;
        gotFields[index] = true;

        return true;
    }

    private void completeRecord(String[] record, boolean[] gotFields) throws IOException {
        // This loop should do time plus the by/over/partition/influencer fields
        int numberOfFields = Math.min(nestingOrder.size() - 1, nestedValues.size());
        if (nestingOrder.size() - 1 != nestedValues.size()) {
            logger.warn("Aggregation inputIndex does not match expectation: expected field order: "
                    + nestingOrder + " actual values: " + nestedValues);
        }
        fieldCount += numberOfFields;
        for (int i = 0; i < numberOfFields; ++i) {
            String fieldName = nestingOrder.get(i);
            Integer index = fieldMap.get(fieldName);
            if (index == null) {
                continue;
            }

            String fieldValue = nestedValues.get(i);
            record[index] = fieldValue;
            gotFields[index] = true;
        }

        // This adds the summary count field
        if (latestDocCount != null) {
            ++fieldCount;
            Integer index = fieldMap.get(SchedulerConfig.DOC_COUNT);
            if (index != null) {
                record[index] = latestDocCount;
                gotFields[index] = true;
            }
        }
    }
}
