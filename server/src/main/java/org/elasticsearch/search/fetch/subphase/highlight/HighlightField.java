/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A field highlighted with its highlighted fragments.
 */
public class HighlightField implements ToXContentFragment, Writeable {

    private String name;

    private Text[] fragments;

    public HighlightField(StreamInput in) throws IOException {
        name = in.readString();
        if (in.readBoolean()) {
            int size = in.readVInt();
            if (size == 0) {
                fragments = Text.EMPTY_ARRAY;
            } else {
                fragments = new Text[size];
                for (int i = 0; i < size; i++) {
                    fragments[i] = in.readText();
                }
            }
        }
    }

    public HighlightField(String name, Text[] fragments) {
        this.name = Objects.requireNonNull(name, "missing highlight field name");
        this.fragments = fragments;
    }

    /**
     * The name of the field highlighted.
     */
    public String name() {
        return name;
    }

    /**
     * The name of the field highlighted.
     */
    public String getName() {
        return name();
    }

    /**
     * The highlighted fragments. {@code null} if failed to highlight (for example, the field is not stored).
     */
    public Text[] fragments() {
        return fragments;
    }

    /**
     * The highlighted fragments. {@code null} if failed to highlight (for example, the field is not stored).
     */
    public Text[] getFragments() {
        return fragments();
    }

    @Override
    public String toString() {
        return "[" + name + "], fragments[" + Arrays.toString(fragments) + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (fragments == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeArray(StreamOutput::writeText, fragments);
        }
    }

    public static HighlightField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        Text[] fragments = null;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            List<Text> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                values.add(new Text(parser.text()));
            }
            fragments = values.toArray(new Text[values.size()]);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            fragments = null;
        } else {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token type [" + token + "]");
        }
        return new HighlightField(fieldName, fragments);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(name);
        if (fragments == null) {
            builder.nullValue();
        } else {
            builder.startArray();
            for (Text fragment : fragments) {
                builder.value(fragment);
            }
            builder.endArray();
        }
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
        HighlightField other = (HighlightField) obj;
        return Objects.equals(name, other.name) && Arrays.equals(fragments, other.fragments);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(name, Arrays.hashCode(fragments));
    }

}
