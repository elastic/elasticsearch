/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.shield.plugin.SecurityPlugin;

import java.util.List;

/**
 *
 */
public class SecurityException extends ElasticsearchException.WithRestHeaders {

    public static final ImmutableMap<String, List<String>> HEADERS = ImmutableMap.<String, List<String>>builder().put("WWW-Authenticate", Lists.newArrayList("Basic realm=\""+ SecurityPlugin.NAME +"\"")).build();

    public SecurityException(String msg) {
        super(msg, HEADERS);
    }

    public SecurityException(String msg, Throwable cause) {
        super(msg, cause, HEADERS);
    }
}
