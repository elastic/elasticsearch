/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3.spi;

/**
 * Storage classes supported by Elasticsearch.
 */
public enum S3StorageClass {
    STANDARD,
    REDUCED_REDUNDANCY,
    STANDARD_IA,
    ONEZONE_IA,
    INTELLIGENT_TIERING,
    GLACIER_IR,
    OUTPOSTS
}
