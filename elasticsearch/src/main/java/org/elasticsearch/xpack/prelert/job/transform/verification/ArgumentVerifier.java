/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.transform.verification;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xpack.prelert.job.transform.TransformConfig;

@FunctionalInterface
public interface ArgumentVerifier
{
    void verify(String argument, TransformConfig tc) throws ElasticsearchParseException;
}
