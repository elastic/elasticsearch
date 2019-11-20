package org.elasticsearch.metrics;

/**
 * @author kyra.wkh
 **/
public class MetricsConstant {
    public static final String SEARCH_TOTAL = "search_total";
    public static final String SEARCH_LATENCY_MILLIS = "search_time_in_millis";
    public static final String COORDINATING = "coordinating";

    public enum MetricsType {
        COUNTER,
        MEAN
    }
}
