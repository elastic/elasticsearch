/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface ColumnInfo extends Writeable {
    /*
    static ColumnInfo fromXContent(XContentParser parser) {
        return ColumnInfoImpl.PARSER.apply(parser, null);
    }

     */

    XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException;

    String name();

    String outputType();
}
