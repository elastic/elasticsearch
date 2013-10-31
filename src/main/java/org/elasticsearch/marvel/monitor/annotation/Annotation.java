/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.monitor.annotation;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class Annotation {

    public final static DateTimeFormatter datePrinter = Joda.forPattern("date_time").printer();

    protected long timestamp;

    public Annotation(long timestamp) {
        this.timestamp = timestamp;
    }

    public long timestamp() {
        return timestamp;
    }

    /**
     * @return annotation's type as a short string without spaces
     */
    public abstract String type();

    /**
     * should return a short string based description of the annotation
     */
    abstract String conciseDescription();

    @Override
    public String toString() {
        return "[" + type() + "] annotation: [" + conciseDescription() + "]";
    }

    public XContentBuilder addXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("@timestamp", datePrinter.print(timestamp));
        builder.field("message", conciseDescription());
        return builder;
    }
}
