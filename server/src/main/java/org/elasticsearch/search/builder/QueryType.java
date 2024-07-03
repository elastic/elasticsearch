/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

public enum QueryType {

    VECTOR("vector"),
    GEO("geo"),
    TERM("term"),
    FULL_TEXT("text"),
    SPAN("span"),
    SPECIALIZED("specialized"),
    JOINING("join");

    public final String type;

    QueryType(String type) {
        this.type = type;
    }
}
