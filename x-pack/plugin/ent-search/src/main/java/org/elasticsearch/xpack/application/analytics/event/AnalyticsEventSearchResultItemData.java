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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;

public class AnalyticsEventSearchResultItemData implements ToXContent, Writeable {

    private static final ConstructingObjectParser<AnalyticsEventSearchResultItemData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>("search_result_item", false, (p, c) -> {
            if (Objects.isNull(p[0]) && Objects.isNull(p[1])) {
                throw new IllegalArgumentException(
                    LoggerMessageFormat.format(
                        "Either [{}}] or [{}] field is required",
                        DOCUMENT_FIELD.getPreferredName(),
                        PAGE_FIELD.getPreferredName()
                    )
                );
            }
            return new AnalyticsEventSearchResultItemData((AnalyticsEventDocumentData) p[0], (AnalyticsEventPageData) p[1]);
        });

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalyticsEventDocumentData::fromXContent, DOCUMENT_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalyticsEventPageData::fromXContent, PAGE_FIELD);
    }

    private final AnalyticsEventDocumentData document;

    private final AnalyticsEventPageData page;

    public AnalyticsEventSearchResultItemData(@Nullable AnalyticsEventDocumentData document, @Nullable AnalyticsEventPageData page) {
        this.document = document;
        this.page = page;
    }

    public AnalyticsEventSearchResultItemData(StreamInput in) throws IOException {
        this(in.readOptionalWriteable(AnalyticsEventDocumentData::new), in.readOptionalWriteable(AnalyticsEventPageData::new));
    }

    public static AnalyticsEventSearchResultItemData fromXContent(XContentParser parser, AnalyticsEvent.Context context)
        throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventDocumentData document() {
        return document;
    }

    public AnalyticsEventPageData page() {
        return page;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (Objects.nonNull(document)) {
                builder.field(DOCUMENT_FIELD.getPreferredName(), document());
            }

            if (Objects.nonNull(page)) {
                builder.field(PAGE_FIELD.getPreferredName(), page());
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(document);
        out.writeOptionalWriteable(page);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchResultItemData that = (AnalyticsEventSearchResultItemData) o;
        return Objects.equals(document, that.document) && Objects.equals(page, that.page);
    }

    @Override
    public int hashCode() {
        return Objects.hash(document, page);
    }
}
