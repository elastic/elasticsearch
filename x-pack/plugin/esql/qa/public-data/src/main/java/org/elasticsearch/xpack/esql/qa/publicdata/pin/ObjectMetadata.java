/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

/**
 * The metadata identity of one remote object, as returned by HTTP {@code HEAD} or S3
 * {@code ListObjectsV2}. This is all a pin ever records — object bodies are never fetched.
 *
 * @param key          object key (S3) or full URI (HTTPS)
 * @param etag         entity tag, quotes stripped; null where the store returns none
 * @param sizeBytes    content length
 * @param lastModified raw last-modified string as reported by the store; informational only
 */
public record ObjectMetadata(String key, String etag, long sizeBytes, String lastModified) {}
