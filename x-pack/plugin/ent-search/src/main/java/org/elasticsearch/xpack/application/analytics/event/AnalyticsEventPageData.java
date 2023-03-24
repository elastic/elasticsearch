/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventPageData implements ToXContent, Writeable {
    public static ParseField PAGE_FIELD = new ParseField("page");

    public static ParseField PAGE_URL_FIELD = new ParseField("url");

    public static ParseField PAGE_TITLE_FIELD = new ParseField("title");

    public static ParseField PAGE_REFERRER_FIELD = new ParseField("referrer");

    private static final ConstructingObjectParser<AnalyticsEventPageData, AnalyticsEvent.Context> PARSER = new ConstructingObjectParser<>(
        "pageview_event",
        false,
        (p, c) -> new AnalyticsEventPageData((String) p[0], (String) p[1], (String) p[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PAGE_URL_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PAGE_TITLE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PAGE_REFERRER_FIELD);
    }

    private final String url;

    private final String title;

    private final String referrer;

    public AnalyticsEventPageData(String url, @Nullable String title, @Nullable String referrer) {
        this.url = Strings.requireNonBlank(url, "page url can't be empty");
        this.title = title;
        this.referrer = referrer;
    }

    public AnalyticsEventPageData(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString(), in.readOptionalString());
    }

    public static AnalyticsEventPageData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String url() {
        return url;
    }

    public String title() {
        return title;
    }

    public String referrer() {
        return referrer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(PAGE_URL_FIELD.getPreferredName(), url());

            if (Objects.nonNull(title)) {
                builder.field(PAGE_TITLE_FIELD.getPreferredName(), title());
            }

            if (Objects.nonNull(referrer)) {
                builder.field(PAGE_REFERRER_FIELD.getPreferredName(), referrer());
            }
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(url);
        out.writeOptionalString(title);
        out.writeOptionalString(referrer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventPageData that = (AnalyticsEventPageData) o;
        return url.equals(that.url) && Objects.equals(title, that.title) && Objects.equals(referrer, that.referrer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, title, referrer);
    }
}
