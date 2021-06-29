/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;


/**
 * An enumeration of the different normalization levels.
 * The string value of each level has to match the equivalent
 * level names in the normalizer C++ process.
 */
enum Level {
    ROOT("root"),
    LEAF("leaf"),
    BUCKET_INFLUENCER("inflb"),
    INFLUENCER("infl"),
    PARTITION("part");

    private final String key;

    Level(String key) {
        this.key = key;
    }

    public String asString() {
        return key;
    }
}
