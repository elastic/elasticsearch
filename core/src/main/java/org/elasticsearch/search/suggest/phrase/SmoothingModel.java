/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.suggest.phrase.WordScorer.WordScorerFactory;

import java.io.IOException;

public abstract class SmoothingModel implements NamedWriteable, ToXContentFragment {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        innerToXContent(builder,params);
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
                if (LinearInterpolation.PARSE_FIELD.match(fieldName)) {
                    model = LinearInterpolation.fromXContent(parser);
                } else if (Laplace.PARSE_FIELD.match(fieldName)) {
                    model = Laplace.fromXContent(parser);
                } else if (StupidBackoff.PARSE_FIELD.match(fieldName)) {
                    model = StupidBackoff.fromXContent(parser);
                } else {
                    throw new IllegalArgumentException("suggester[phrase] doesn't support object field [" + fieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "[smoothing] unknown token [" + token + "] after [" + fieldName + "]");
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