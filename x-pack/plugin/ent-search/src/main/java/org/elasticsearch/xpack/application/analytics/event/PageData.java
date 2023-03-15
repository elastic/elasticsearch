/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PageData implements Writeable, ToXContentObject {
    public static final ParseField URL_FIELD = new ParseField("url");
    public static final ParseField TITLE_FIELD = new ParseField("title");
    public static final ParseField REFERRER_FIELD = new ParseField("referrer");

    public static final ConstructingObjectParser<PageData, Void> PARSER = new ConstructingObjectParser<>(
        "event_page_data",
        false,
        a -> new PageData((String) a[0], (String) a[1], (String) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), URL_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TITLE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REFERRER_FIELD);
    }

    private final String url;
    private final String title;
    private final String referrer;

    public PageData(String url, String title, String referrer) {
        this.url = url;
        this.title = title;
        this.referrer = referrer;
    }

    public PageData(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(url);
        out.writeOptionalString(title);
        out.writeOptionalString(referrer);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(URL_FIELD.getPreferredName(), url);
            builder.field(TITLE_FIELD.getPreferredName(), title);
            builder.field(REFERRER_FIELD.getPreferredName(), referrer);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageData pageData = (PageData) o;
        return Objects.equals(url, pageData.url) && Objects.equals(title, pageData.title) && Objects.equals(referrer, pageData.referrer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, title, referrer);
    }
}
