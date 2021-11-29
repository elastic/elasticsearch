/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ForceMergeAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "forcemerge";
    private static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME, true, a -> {
        int maxNumSegments = (int) a[0];
        return new ForceMergeAction(maxNumSegments);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
    }

    private final int maxNumSegments;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName() + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ForceMergeAction other = (ForceMergeAction) obj;
        return Objects.equals(maxNumSegments, other.maxNumSegments);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
