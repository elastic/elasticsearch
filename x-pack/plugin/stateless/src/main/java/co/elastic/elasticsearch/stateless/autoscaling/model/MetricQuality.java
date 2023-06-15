/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

public enum MetricQuality {
    EXACT((byte) 0, "exact"),
    MINIMUM((byte) 1, "minimum");

    private final byte id;
    private final String label;

    MetricQuality(byte value, String label) {
        this.id = value;
        this.label = label;
    }

    public byte getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public static MetricQuality fromId(byte id) {
        return switch (id) {
            case 0 -> EXACT;
            case 1 -> MINIMUM;
            default -> throw new IllegalStateException("No metric quality for [" + id + "]");
        };
    }
}
