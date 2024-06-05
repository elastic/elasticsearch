/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.search.suggest.phrase.WordScorer.WordScorerFactory;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public abstract class SmoothingModel implements VersionedNamedWriteable, ToXContentFragment {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SmoothingModel other = (SmoothingModel) obj;
        return doEquals(other);
    }

    @Override
    public final int hashCode() {
        /*
         * Override hashCode here and forward to an abstract method to force
         * extensions of this class to override hashCode in the same way that we
         * force them to override equals. This also prevents false positives in
         * CheckStyle's EqualsHashCode check.
         */
        return doHashCode();
    }

    protected abstract int doHashCode();

    public static SmoothingModel fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        SmoothingModel model = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (LinearInterpolation.PARSE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    model = LinearInterpolation.fromXContent(parser);
                } else if (Laplace.PARSE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    model = Laplace.fromXContent(parser);
                } else if (StupidBackoff.PARSE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    model = StupidBackoff.fromXContent(parser);
                } else {
                    throw new IllegalArgumentException("suggester[phrase] doesn't support object field [" + fieldName + "]");
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[smoothing] unknown token [" + token + "] after [" + fieldName + "]"
                );
            }
        }
        return model;
    }

    public abstract WordScorerFactory buildWordScorerFactory();

    /**
     * subtype specific implementation of "equals".
     */
    protected abstract boolean doEquals(SmoothingModel other);

    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
}
