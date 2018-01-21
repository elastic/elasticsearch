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

package org.elasticsearch.search.rescore;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;

import java.io.IOException;
import java.util.Objects;

/**
 * The abstract base builder for instances of {@link RescorerBuilder}.
 */
public abstract class RescorerBuilder<RB extends RescorerBuilder<RB>>
        implements NamedWriteable, ToXContentObject, Rewriteable<RescorerBuilder<RB>> {
    public static final int DEFAULT_WINDOW_SIZE = 10;

    protected Integer windowSize;

    private static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    /**
     * Construct an empty RescoreBuilder.
     */
    public RescorerBuilder() {
    }

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

    public static RescorerBuilder<?> parseFromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        RescorerBuilder<?> rescorer = null;
        Integer windowSize = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_FIELD.match(fieldName)) {
                    windowSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                rescorer = parser.namedObject(RescorerBuilder.class, fieldName, null);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        if (rescorer == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing rescore type");
        }
        if (windowSize != null) {
            rescorer.windowSize(windowSize.intValue());
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

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Build the {@linkplain RescoreContext} that will be used to actually
     * execute the rescore against a particular shard.
     */
    public final RescoreContext buildContext(QueryShardContext context) throws IOException {
        int finalWindowSize = windowSize == null ? DEFAULT_WINDOW_SIZE : windowSize;
        RescoreContext rescoreContext = innerBuildContext(finalWindowSize, context);
        return rescoreContext;
    }

    /**
     * Extensions override this to build the context that they need for rescoring.
     */
    protected abstract RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException;

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
