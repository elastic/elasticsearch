/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class RolloverAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "rollover";
    private static final ParseField MAX_SIZE_FIELD = new ParseField("max_size");
    private static final ParseField MAX_SINGLE_PRIMARY_SIZE_FIELD = new ParseField("max_single_primary_size");
    private static final ParseField MAX_AGE_FIELD = new ParseField("max_age");
    private static final ParseField MAX_DOCS_FIELD = new ParseField("max_docs");

    private static final ConstructingObjectParser<RolloverAction, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
        a -> new RolloverAction((ByteSizeValue) a[0], (ByteSizeValue) a[1], (TimeValue) a[2], (Long) a[3]));

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SIZE_FIELD.getPreferredName()),
            MAX_SIZE_FIELD, ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SINGLE_PRIMARY_SIZE_FIELD.getPreferredName()),
            MAX_SINGLE_PRIMARY_SIZE_FIELD, ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_AGE_FIELD.getPreferredName()),
            MAX_AGE_FIELD, ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_DOCS_FIELD);
    }

    private final ByteSizeValue maxSize;
    private final ByteSizeValue maxSinglePrimarySize;
    private final TimeValue maxAge;
    private final Long maxDocs;

    public static RolloverAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RolloverAction(ByteSizeValue maxSize, ByteSizeValue maxSinglePrimarySize, TimeValue maxAge, Long maxDocs) {
        if (maxSize == null && maxSinglePrimarySize == null && maxAge == null && maxDocs == null) {
            throw new IllegalArgumentException("At least one rollover condition must be set.");
        }
        this.maxSize = maxSize;
        this.maxSinglePrimarySize = maxSinglePrimarySize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    public ByteSizeValue getMaxSize() {
        return maxSize;
    }

    public ByteSizeValue getMaxSinglePrimarySize() {
        return maxSinglePrimarySize;
    }

    public TimeValue getMaxAge() {
        return maxAge;
    }

    public Long getMaxDocs() {
        return maxDocs;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxSize != null) {
            builder.field(MAX_SIZE_FIELD.getPreferredName(), maxSize.getStringRep());
        }
        if (maxSinglePrimarySize != null) {
            builder.field(MAX_SINGLE_PRIMARY_SIZE_FIELD.getPreferredName(), maxSinglePrimarySize.getStringRep());
        }
        if (maxAge != null) {
            builder.field(MAX_AGE_FIELD.getPreferredName(), maxAge.getStringRep());
        }
        if (maxDocs != null) {
            builder.field(MAX_DOCS_FIELD.getPreferredName(), maxDocs);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSize, maxSinglePrimarySize, maxAge, maxDocs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverAction other = (RolloverAction) obj;
        return Objects.equals(maxSize, other.maxSize) &&
            Objects.equals(maxSinglePrimarySize, other.maxSinglePrimarySize) &&
            Objects.equals(maxAge, other.maxAge) &&
            Objects.equals(maxDocs, other.maxDocs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
