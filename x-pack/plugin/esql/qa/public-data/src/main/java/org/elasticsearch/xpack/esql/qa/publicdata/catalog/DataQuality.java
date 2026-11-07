/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * Data-quality dimension. {@link #SCHEMA_DRIFT} corpora carry genuine real-world dirt and must
 * return <em>correct</em> answers; {@link #MISLABELED}/{@link #MISPOINTED} mark deliberately wrong
 * configurations over real pinned objects, exercised by the expected-failure IT.
 */
public enum DataQuality {
    CLEAN("clean"),
    SCHEMA_DRIFT("schema-drift"),
    MISLABELED("mislabeled"),
    MISPOINTED("mispointed");

    private final String id;

    DataQuality(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static DataQuality fromId(String id) {
        for (DataQuality quality : values()) {
            if (quality.id.equals(id.toLowerCase(Locale.ROOT))) {
                return quality;
            }
        }
        throw new IllegalArgumentException("Unknown quality [" + id + "]; expected one of clean, schema-drift, mislabeled, mispointed");
    }
}
