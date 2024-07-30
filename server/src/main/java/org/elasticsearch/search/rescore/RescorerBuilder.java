/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The abstract base builder for instances of {@link RescorerBuilder}.
 */
public abstract class RescorerBuilder<RB extends RescorerBuilder<RB>>
    implements
        VersionedNamedWriteable,
        ToXContentObject,
        Rewriteable<RescorerBuilder<RB>> {
    public static final int DEFAULT_WINDOW_SIZE = 10;

    protected Integer windowSize;

    private static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    /**
     * Construct an empty RescoreBuilder.
     */
    public RescorerBuilder() {}

    /**
     * Read from a stream.
     */
    protected RescorerBuilder(StreamInput in) throws IOException {
        windowSize = in.readOptionalVInt();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(this.windowSize);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    public RB windowSize(int windowSize) {
        this.windowSize = windowSize;
        return (RB) this;
    }

    public Integer windowSize() {
        return windowSize;
    }

    public static RescorerBuilder<?> parseFromXContent(XContentParser parser, Consumer<String> rescorerNameConsumer) throws IOException {
        String fieldName = null;
        RescorerBuilder<?> rescorer = null;
        Integer windowSize = null;
        XContentParser.Token token;
        String rescorerType = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    windowSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    rescorer = parser.namedObject(RescorerBuilder.class, fieldName, null);
                    rescorerNameConsumer.accept(fieldName);
                    rescorerType = fieldName;
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        if (rescorer == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing rescore type");
        }

        if (windowSize != null) {
            rescorer.windowSize(windowSize.intValue());
        } else if (rescorer.isWindowSizeRequired()) {
            throw new ParsingException(parser.getTokenLocation(), "window_size is required for rescorer of type [" + rescorerType + "]");
        }

        return rescorer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (windowSize != null) {
            builder.field("window_size", windowSize);
        }
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public ActionRequestValidationException validate(SearchSourceBuilder source, ActionRequestValidationException validationException) {
        return validationException;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Indicate if the window_size is a required parameter for the rescorer.
     */
    protected boolean isWindowSizeRequired() {
        return false;
    }

    /**
     * Build the {@linkplain RescoreContext} that will be used to actually
     * execute the rescore against a particular shard.
     */
    public final RescoreContext buildContext(SearchExecutionContext context) throws IOException {
        if (isWindowSizeRequired()) {
            assert windowSize != null;
        }
        int finalWindowSize = windowSize == null ? DEFAULT_WINDOW_SIZE : windowSize;

        return innerBuildContext(finalWindowSize, context);
    }

    /**
     * Extensions override this to build the context that they need for rescoring.
     */
    protected abstract RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(windowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RescorerBuilder<?> other = (RescorerBuilder<?>) obj;
        return Objects.equals(windowSize, other.windowSize);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
