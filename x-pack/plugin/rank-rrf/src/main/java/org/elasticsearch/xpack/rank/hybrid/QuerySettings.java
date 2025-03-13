/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface QuerySettings extends NamedXContentObject {
    ParseField TYPE_FIELD = new ParseField("type");

    QueryType getQueryType();

    QueryBuilder constructQueryBuilder(String field, String query);

    @Override
    default String getName() {
        return getQueryType().getQueryName();
    }
}
