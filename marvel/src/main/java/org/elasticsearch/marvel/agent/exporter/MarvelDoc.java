/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public abstract class MarvelDoc<T> implements ToXContent {

    private final String clusterName;
    private final String type;
    private final long timestamp;

    public MarvelDoc(String clusterName, String type, long timestamp) {
        this.clusterName = clusterName;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String clusterName() {
        return clusterName;
    }

    public String type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }

    public abstract T payload();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.CLUSTER_NAME, clusterName());
        DateTime timestampDateTime = new DateTime(timestamp(), DateTimeZone.UTC);
        builder.field(Fields.TIMESTAMP, timestampDateTime.toString());
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
    }
}