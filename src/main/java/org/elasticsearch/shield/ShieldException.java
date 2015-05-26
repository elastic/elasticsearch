/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchException;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;

/**
 *
 */
public class ShieldException extends ElasticsearchException.WithRestHeaders {

    public static final Tuple<String, String[]> BASIC_AUTH_HEADER = Tuple.tuple("WWW-Authenticate", new String[]{"Basic realm=\"" + ShieldPlugin.NAME + "\""});

    public ShieldException(String msg) {
        super(msg, BASIC_AUTH_HEADER);
    }

    public ShieldException(String msg, Throwable cause) {
        super(msg, BASIC_AUTH_HEADER);
        initCause(cause);
    }
}
