/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.util.ArrayList;
import java.util.List;

public abstract class RankContextBuilder implements Writeable, ToXContent {

    protected final List<QueryBuilder> queryBuilders = new ArrayList<>();
    protected int size;
    protected int from;

    public List<QueryBuilder> queryBuilders() {
        return queryBuilders;
    }

    public RankContextBuilder size(int size) {
        this.size = size;
        return this;
    }

    public RankContextBuilder from(int from) {
        this.from = from;
        return this;
    }

    public abstract ParseField name();

    public abstract RankContext build();
}
