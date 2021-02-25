/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderExtension;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

/**
 * Extension for SQL's JDBC specific classes that need to be
 * encoded by {@link XContentBuilder} in a specific way.
 */
public class XContentSqlExtension implements XContentBuilderExtension {

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        return Map.of(
                Date.class, (b, v) -> b.value(((Date) v).getTime()),
                ZonedDateTime.class, (b, v) -> b.value(StringUtils.toString(v)));
    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        return Collections.emptyMap();
    }

    @Override
    public Map<Class<?>, Function<Object, Object>> getDateTransformers() {
        return Map.of(
                Date.class, d -> ((Date) d).getTime(),
                ZonedDateTime.class, StringUtils::toString);
    }
}
