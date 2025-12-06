/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

/**
 * Implements the Lucene 7.0 index format. Will be loaded via SPI for indices created/written with Lucene 7.x (Elasticsearch 6.x) mounted
 * as archive indices in Elasticsearch 9.x. Note that for indices with same version mounted first as archive indices in Elasticsearch 8.x,
 * {@link BWCLucene70Codec} will be instead used which provides the same functionality, only registered with a different name.
 *
 * @deprecated Only for 7.0 back compat
 */
@Deprecated
public class Lucene70Codec extends BWCLucene70Codec {

    public Lucene70Codec() {
        super("Lucene70");
    }
}
