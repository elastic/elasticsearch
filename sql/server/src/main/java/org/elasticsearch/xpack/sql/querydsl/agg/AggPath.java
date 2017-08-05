/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.util.StringUtils;

// utility class around constructing and parsing ES specific aggregation path
// see org.elasticsearch.search.aggregations.support.AggregationPath
public abstract class AggPath {

    private static final char PATH_DELIMITER_CHAR = '>';
    private static final String PATH_DELIMITER = String.valueOf(PATH_DELIMITER_CHAR);
    private static final String VALUE_DELIMITER = ".";
    private static final String PATH_BUCKET_VALUE = "._key";
    private static final String PATH_BUCKET_COUNT = "._count";
    private static final String PATH_BUCKET_VALUE_FORMATTED = "._key_as_string";
    private static final String PATH_DEFAULT_VALUE = ".value";

    public static String bucketCount(String aggPath) {
        return aggPath + PATH_BUCKET_COUNT;
    }

    public static String bucketValue(String aggPath) {
        return aggPath + PATH_BUCKET_VALUE;
    }

    public static boolean isBucketValueFormatted(String path) {
        return path.endsWith(PATH_BUCKET_VALUE_FORMATTED);
    }

    public static String bucketValueWithoutFormat(String path) {
        return path.substring(0, path.length() - PATH_BUCKET_VALUE_FORMATTED.length());
    }

    public static String metricValue(String aggPath) {
        return aggPath + PATH_DEFAULT_VALUE;
    }

    public static String metricValue(String aggPath, String valueName) {
        // handle aggPath inconsistency (for percentiles and percentileRanks) percentile[99.9] (valid) vs percentile.99.9 (invalid)
        return valueName.startsWith("[") ? aggPath + valueName : aggPath + VALUE_DELIMITER + valueName;
    }

    public static String path(String parent, String child) {
        return (Strings.hasLength(parent) ? parent + PATH_DELIMITER : StringUtils.EMPTY) + child;
    }

    //
    // The depth indicates the level of an agg excluding the root agg (because all aggs in SQL require a group). However all other bucket aggs are counted.
    // Since the path does not indicate the type of agg used, to differentiate between metric properties and bucket properties, the bucket value is considered.
    // This is needed since one might refer to the keys or count of a bucket path.
    // As the opposite side there are metric aggs which have the same level as their parent (their nesting is an ES implementation detail)

    // Default examples:
    //
    // agg1 = 0                         ; agg1 = default/root group 
    // agg1>agg2._count = 1             ; ._count indicates agg2 is a bucket agg and thus it counted - agg1 (default group), depth=0, agg2 (bucketed), depth=1 
    // agg1>agg2>agg3.value = 1         ; agg3.value indicates a metric bucket thus only agg1 and agg2 are counted -> depth=2. In other words, agg3.value has the same depth as agg2._count
    // agg1>agg2>agg3._count = 2        ; ._count indicates agg3 is a bucket agg, so count it for depth -> depth = 2
    // agg1>agg2>agg3.sum = 1           ; .sum indicates agg3 is a metric agg, only agg1 and agg2 are bucket and with agg1 being the default group -> depth = 1
    public static int depth(String path) {
        int depth = countCharIn(path, PATH_DELIMITER_CHAR);
        // a metric value always has .foo while a bucket prop with ._foo
        int dot = path.lastIndexOf(".");
        if (depth > 0 && dot > 0) {
            String prop = path.substring(dot + 1);
            if (!prop.startsWith("_")) {
                return Math.max(0, depth - 1);
            }
        }
        return depth;
    }

    private static int countCharIn(CharSequence sequence, char c) {
        int count = 0;
        for (int i = 0; i < sequence.length(); i++) {
            if (c == sequence.charAt(i)) {
                count++;
            }
        }
        return count;
    }
}