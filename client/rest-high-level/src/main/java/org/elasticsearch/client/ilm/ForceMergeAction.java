/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
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
    public static final ParseField CODEC = new ParseField("index_codec");
    public static final ParseField ONLY_EXPUNGE_DELETES = new ParseField("only_expunge_deletes");

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME, true, a -> {
        Integer maxNumSegments = (Integer) a[0];
        String codec = a[1] != null ? (String) a[1] : null;
        boolean onlyExpungeDeletes = a[2] != null && (boolean) a[2];
        return new ForceMergeAction(maxNumSegments, codec, onlyExpungeDeletes);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CODEC);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ONLY_EXPUNGE_DELETES);
    }

    private final Integer maxNumSegments;
    private final String codec;
    private final boolean onlyExpungeDeletes;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(@Nullable Integer maxNumSegments, @Nullable String codec, boolean onlyExpungeDeletes) {
        if (maxNumSegments != null && onlyExpungeDeletes) {
            throw new IllegalArgumentException(
                "cannot set [max_num_segments] and [only_expunge_deletes] at the same time,"
                    + " those two parameters are mutually exclusive"
            );
        }
        if (maxNumSegments == null && onlyExpungeDeletes == false) {
            throw new IllegalArgumentException("Either [max_num_segments] or [only_expunge_deletes] must be set");
        }
        if (maxNumSegments != null && maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName() + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        if (codec != null && CodecService.BEST_COMPRESSION_CODEC.equals(codec) == false) {
            throw new IllegalArgumentException("unknown index codec: [" + codec + "]");
        }
        this.codec = codec;
        this.onlyExpungeDeletes = onlyExpungeDeletes;
    }

    public Integer getMaxNumSegments() {
        return maxNumSegments;
    }

    public String getCodec() {
        return this.codec;
    }

    public boolean getOnlyExpungeDeletes() {
        return this.onlyExpungeDeletes;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxNumSegments != null) {
            builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        }
        if (codec != null) {
            builder.field(CODEC.getPreferredName(), codec);
        }
        builder.field(ONLY_EXPUNGE_DELETES.getPreferredName(), onlyExpungeDeletes);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, codec, onlyExpungeDeletes);
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
        return Objects.equals(this.maxNumSegments, other.maxNumSegments)
            && Objects.equals(this.codec, other.codec)
            && Objects.equals(this.onlyExpungeDeletes, other.onlyExpungeDeletes);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
