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

package org.elasticsearch.search.highlight;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A builder for search highlighting. Settings can control how large fields
 * are summarized to show only selected snippets ("fragments") containing search terms.
 *
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#highlight()
 */
public class HighlightBuilder extends AbstractHighlighterBuilder<HighlightBuilder> implements Writeable<HighlightBuilder>, ToXContent  {

    public static final HighlightBuilder PROTOTYPE = new HighlightBuilder();

    private final List<Field> fields = new ArrayList<>();

    private String encoder;

    private boolean useExplicitFieldOrder = false;

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5 using the default encoder
     *
     * @param name The field to highlight
     */
    public HighlightBuilder field(String name) {
        return field(new Field(name));
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * default number of fragments of 5.
     *
     * @param name         The field to highlight
     * @param fragmentSize The size of a fragment in characters
     */
    public HighlightBuilder field(String name, int fragmentSize) {
        return field(new Field(name).fragmentSize(fragmentSize));
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public HighlightBuilder field(String name, int fragmentSize, int numberOfFragments) {
        return field(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments));
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     * @param fragmentOffset    The offset from the start of the fragment to the start of the highlight
     */
    public HighlightBuilder field(String name, int fragmentSize, int numberOfFragments, int fragmentOffset) {
        return field(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments)
                .fragmentOffset(fragmentOffset));
    }

    public HighlightBuilder field(Field field) {
        fields.add(field);
        return this;
    }

    public List<Field> fields() {
        return this.fields;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allowed schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public HighlightBuilder tagsSchema(String schemaName) {
        switch (schemaName) {
        case "default":
            preTags(HighlighterParseElement.DEFAULT_PRE_TAGS);
            postTags(HighlighterParseElement.DEFAULT_POST_TAGS);
            break;
        case "styled":
            preTags(HighlighterParseElement.STYLED_PRE_TAG);
            postTags(HighlighterParseElement.STYLED_POST_TAGS);
            break;
        default:
            throw new IllegalArgumentException("Unknown tag schema ["+ schemaName +"]");
        }
        return this;
    }

    /**
     * Set encoder for the highlighting
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param encoder name
     */
    public HighlightBuilder encoder(String encoder) {
        this.encoder = encoder;
        return this;
    }

    /**
     * Getter for {@link #encoder(String)}
     */
    public String encoder() {
        return this.encoder;
    }

    /**
     * Send the fields to be highlighted using a syntax that is specific about the order in which they should be highlighted.
     * @return this for chaining
     */
    public HighlightBuilder useExplicitFieldOrder(boolean useExplicitFieldOrder) {
        this.useExplicitFieldOrder = useExplicitFieldOrder;
        return this;
    }

    /**
     * Gets value set with {@link #useExplicitFieldOrder(boolean)}
     */
    public Boolean useExplicitFieldOrder() {
        return this.useExplicitFieldOrder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("highlight");
        innerXContent(builder);
        builder.endObject();
        return builder;
    }

    /**
     * Creates a new {@link HighlightBuilder} from the highlighter held by the {@link QueryParseContext}
     * in {@link org.elasticsearch.common.xcontent.XContent} format
     *
     * @param parseContext
     *            the input parse context. The state on the parser contained in
     *            this context will be changed as a side effect of this method
     *            call
     * @return the new {@link HighlightBuilder}
     */
    public static HighlightBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token;
        String topLevelFieldName = null;

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                    List<String> preTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        preTagsList.add(parser.text());
                    }
                    highlightBuilder.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                    List<String> postTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        postTagsList.add(parser.text());
                    }
                    highlightBuilder.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                } else if ("fields".equals(topLevelFieldName)) {
                    highlightBuilder.useExplicitFieldOrder(true);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.START_OBJECT) {
                            String highlightFieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    if (highlightFieldName != null) {
                                        throw new IllegalArgumentException("If highlighter fields is an array it must contain objects containing a single field");
                                    }
                                    highlightFieldName = parser.currentName();
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    highlightBuilder.field(Field.fromXContent(highlightFieldName, parseContext));
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("If highlighter fields is an array it must contain objects containing a single field");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("order".equals(topLevelFieldName)) {
                    highlightBuilder.order(parser.text());
                } else if ("tags_schema".equals(topLevelFieldName) || "tagsSchema".equals(topLevelFieldName)) {
                    highlightBuilder.tagsSchema(parser.text());
                } else if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    highlightBuilder.highlightFilter(parser.booleanValue());
                } else if ("fragment_size".equals(topLevelFieldName) || "fragmentSize".equals(topLevelFieldName)) {
                    highlightBuilder.fragmentSize(parser.intValue());
                } else if ("number_of_fragments".equals(topLevelFieldName) || "numberOfFragments".equals(topLevelFieldName)) {
                    highlightBuilder.numOfFragments(parser.intValue());
                } else if ("encoder".equals(topLevelFieldName)) {
                    highlightBuilder.encoder(parser.text());
                } else if ("require_field_match".equals(topLevelFieldName) || "requireFieldMatch".equals(topLevelFieldName)) {
                    highlightBuilder.requireFieldMatch(parser.booleanValue());
                } else if ("boundary_max_scan".equals(topLevelFieldName) || "boundaryMaxScan".equals(topLevelFieldName)) {
                    highlightBuilder.boundaryMaxScan(parser.intValue());
                } else if ("boundary_chars".equals(topLevelFieldName) || "boundaryChars".equals(topLevelFieldName)) {
                    highlightBuilder.boundaryChars(parser.text().toCharArray());
                } else if ("type".equals(topLevelFieldName)) {
                    highlightBuilder.highlighterType(parser.text());
                } else if ("fragmenter".equals(topLevelFieldName)) {
                    highlightBuilder.fragmenter(parser.text());
                } else if ("no_match_size".equals(topLevelFieldName) || "noMatchSize".equals(topLevelFieldName)) {
                    highlightBuilder.noMatchSize(parser.intValue());
                } else if ("force_source".equals(topLevelFieldName) || "forceSource".equals(topLevelFieldName)) {
                    highlightBuilder.forceSource(parser.booleanValue());
                } else if ("phrase_limit".equals(topLevelFieldName) || "phraseLimit".equals(topLevelFieldName)) {
                    highlightBuilder.phraseLimit(parser.intValue());
                }
            } else if (token == XContentParser.Token.START_OBJECT && "options".equals(topLevelFieldName)) {
                highlightBuilder.options(parser.map());
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("fields".equals(topLevelFieldName)) {
                    String highlightFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            highlightFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            highlightBuilder.field(Field.fromXContent(highlightFieldName, parseContext));
                        }
                    }
                } else if ("highlight_query".equals(topLevelFieldName) || "highlightQuery".equals(topLevelFieldName)) {
                    highlightBuilder.highlightQuery(parseContext.parseInnerQueryBuilder());
                }
            }
        }

        if (highlightBuilder.preTags() != null && highlightBuilder.postTags() == null) {
            throw new IllegalArgumentException("Highlighter global preTags are set, but global postTags are not set");
        }
        return highlightBuilder;
    }



    public void innerXContent(XContentBuilder builder) throws IOException {
        // first write common options
        commonOptionsToXContent(builder);
        // special options for top-level highlighter
        if (encoder != null) {
            builder.field("encoder", encoder);
        }
        if (fields.size() > 0) {
            if (useExplicitFieldOrder) {
                builder.startArray("fields");
            } else {
                builder.startObject("fields");
            }
            for (Field field : fields) {
                if (useExplicitFieldOrder) {
                    builder.startObject();
                }
                field.innerXContent(builder);
                if (useExplicitFieldOrder) {
                    builder.endObject();
                }
            }
            if (useExplicitFieldOrder) {
                builder.endArray();
            } else {
                builder.endObject();
            }
        }
    }

    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(encoder, useExplicitFieldOrder, fields);
    }

    @Override
    protected boolean doEquals(HighlightBuilder other) {
        return Objects.equals(encoder, other.encoder) &&
                Objects.equals(useExplicitFieldOrder, other.useExplicitFieldOrder) &&
                Objects.equals(fields, other.fields);
    }

    @Override
    public HighlightBuilder readFrom(StreamInput in) throws IOException {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.readOptionsFrom(in)
                .encoder(in.readOptionalString())
                .useExplicitFieldOrder(in.readBoolean());
        int fields = in.readVInt();
        for (int i = 0; i < fields; i++) {
            highlightBuilder.field(Field.PROTOTYPE.readFrom(in));
        }
        return highlightBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeOptionsTo(out);
        out.writeOptionalString(encoder);
        out.writeBoolean(useExplicitFieldOrder);
        out.writeVInt(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).writeTo(out);
        }
    }

    public static class Field extends AbstractHighlighterBuilder<Field> implements Writeable<Field> {
        static final Field PROTOTYPE = new Field("_na_");

        private final String name;

        int fragmentOffset = -1;

        String[] matchedFields;

        public Field(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        public Field fragmentOffset(int fragmentOffset) {
            this.fragmentOffset = fragmentOffset;
            return this;
        }

        /**
         * Set the matched fields to highlight against this field data.  Default to null, meaning just
         * the named field.  If you provide a list of fields here then don't forget to include name as
         * it is not automatically included.
         */
        public Field matchedFields(String... matchedFields) {
            this.matchedFields = matchedFields;
            return this;
        }

        public void innerXContent(XContentBuilder builder) throws IOException {
            builder.startObject(name);
            // write common options
            commonOptionsToXContent(builder);
            // write special field-highlighter options
            if (fragmentOffset != -1) {
                builder.field("fragment_offset", fragmentOffset);
            }
            if (matchedFields != null) {
                builder.field("matched_fields", matchedFields);
            }
            builder.endObject();
        }

        private static HighlightBuilder.Field fromXContent(String fieldname, QueryParseContext parseContext) throws IOException {
            XContentParser parser = parseContext.parser();
            XContentParser.Token token;

            final HighlightBuilder.Field field = new HighlightBuilder.Field(fieldname);
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("pre_tags".equals(currentFieldName) || "preTags".equals(currentFieldName)) {
                        List<String> preTagsList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            preTagsList.add(parser.text());
                        }
                        field.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                    } else if ("post_tags".equals(currentFieldName) || "postTags".equals(currentFieldName)) {
                        List<String> postTagsList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            postTagsList.add(parser.text());
                        }
                        field.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                    } else if ("matched_fields".equals(currentFieldName) || "matchedFields".equals(currentFieldName)) {
                        List<String> matchedFields = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            matchedFields.add(parser.text());
                        }
                        field.matchedFields(matchedFields.toArray(new String[matchedFields.size()]));
                    }
                } else if (token.isValue()) {
                    if ("fragment_size".equals(currentFieldName) || "fragmentSize".equals(currentFieldName)) {
                        field.fragmentSize(parser.intValue());
                    } else if ("number_of_fragments".equals(currentFieldName) || "numberOfFragments".equals(currentFieldName)) {
                        field.numOfFragments(parser.intValue());
                    } else if ("fragment_offset".equals(currentFieldName) || "fragmentOffset".equals(currentFieldName)) {
                        field.fragmentOffset(parser.intValue());
                    } else if ("highlight_filter".equals(currentFieldName) || "highlightFilter".equals(currentFieldName)) {
                        field.highlightFilter(parser.booleanValue());
                    } else if ("order".equals(currentFieldName)) {
                        field.order(parser.text());
                    } else if ("require_field_match".equals(currentFieldName) || "requireFieldMatch".equals(currentFieldName)) {
                        field.requireFieldMatch(parser.booleanValue());
                    } else if ("boundary_max_scan".equals(currentFieldName) || "boundaryMaxScan".equals(currentFieldName)) {
                        field.boundaryMaxScan(parser.intValue());
                    } else if ("boundary_chars".equals(currentFieldName) || "boundaryChars".equals(currentFieldName)) {
                        field.boundaryChars(parser.text().toCharArray());
                    } else if ("type".equals(currentFieldName)) {
                        field.highlighterType(parser.text());
                    } else if ("fragmenter".equals(currentFieldName)) {
                        field.fragmenter(parser.text());
                    } else if ("no_match_size".equals(currentFieldName) || "noMatchSize".equals(currentFieldName)) {
                        field.noMatchSize(parser.intValue());
                    } else if ("force_source".equals(currentFieldName) || "forceSource".equals(currentFieldName)) {
                        field.forceSource(parser.booleanValue());
                    } else if ("phrase_limit".equals(currentFieldName) || "phraseLimit".equals(currentFieldName)) {
                        field.phraseLimit(parser.intValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("highlight_query".equals(currentFieldName) || "highlightQuery".equals(currentFieldName)) {
                        field.highlightQuery(parseContext.parseInnerQueryBuilder());
                    } else if ("options".equals(currentFieldName)) {
                        field.options(parser.map());
                    }
                }
            }
            return field;
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(name, fragmentOffset, Arrays.hashCode(matchedFields));
        }

        @Override
        protected boolean doEquals(Field other) {
            return Objects.equals(name, other.name) &&
                    Objects.equals(fragmentOffset, other.fragmentOffset) &&
                    Arrays.equals(matchedFields, other.matchedFields);
        }

        @Override
        public Field readFrom(StreamInput in) throws IOException {
            Field field = new Field(in.readString());
            field.fragmentOffset(in.readVInt());
            field.matchedFields(in.readOptionalStringArray());
            field.readOptionsFrom(in);
            return field;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(fragmentOffset);
            out.writeOptionalStringArray(matchedFields);
            writeOptionsTo(out);
        }
    }
}
