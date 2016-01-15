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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.highlight.SearchContextHighlight.FieldOptions;
import org.elasticsearch.search.highlight.SearchContextHighlight.FieldOptions.Builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * A builder for search highlighting. Settings can control how large fields
 * are summarized to show only selected snippets ("fragments") containing search terms.
 *
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#highlight()
 */
public class HighlightBuilder extends AbstractHighlighterBuilder<HighlightBuilder> implements Writeable<HighlightBuilder>, ToXContent  {

    public static final HighlightBuilder PROTOTYPE = new HighlightBuilder();

    public static final String HIGHLIGHT_ELEMENT_NAME = "highlight";

    /** default for whether to highlight fields based on the source even if stored separately */
    public static final boolean DEFAULT_FORCE_SOURCE = false;
    /** default for whether a field should be highlighted only if a query matches that field */
    public static final boolean DEFAULT_REQUIRE_FIELD_MATCH = true;
    /** default for whether <tt>fvh</tt> should provide highlighting on filter clauses */
    public static final boolean DEFAULT_HIGHLIGHT_FILTER = false;
    /** default for highlight fragments being ordered by score */
    public static final boolean DEFAULT_SCORE_ORDERED = false;
    /** the default encoder setting */
    public static final String DEFAULT_ENCODER = "default";
    /** default for the maximum number of phrases the fvh will consider */
    public static final int DEFAULT_PHRASE_LIMIT = 256;
    /** default for fragment size when there are no matches */
    public static final int DEFAULT_NO_MATCH_SIZE = 0;
    /** the default number of fragments for highlighting */
    public static final int DEFAULT_NUMBER_OF_FRAGMENTS = 5;
    /** the default number of fragments size in characters */
    public static final int DEFAULT_FRAGMENT_CHAR_SIZE = 100;
    /** the default opening tag  */
    public static final String[] DEFAULT_PRE_TAGS = new String[]{"<em>"};
    /** the default closing tag  */
    public static final String[] DEFAULT_POST_TAGS = new String[]{"</em>"};

    /** the default opening tags when <tt>tag_schema = "styled"</tt>  */
    public static final String[] DEFAULT_STYLED_PRE_TAG = {
            "<em class=\"hlt1\">", "<em class=\"hlt2\">", "<em class=\"hlt3\">",
            "<em class=\"hlt4\">", "<em class=\"hlt5\">", "<em class=\"hlt6\">",
            "<em class=\"hlt7\">", "<em class=\"hlt8\">", "<em class=\"hlt9\">",
            "<em class=\"hlt10\">"
    };
    /** the default closing tags when <tt>tag_schema = "styled"</tt>  */
    public static final String[] DEFAULT_STYLED_POST_TAGS = {"</em>"};

    /**
     * a {@link FieldOptions.Builder} with default settings
     */
    public final static Builder defaultFieldOptions() {
        return new SearchContextHighlight.FieldOptions.Builder()
                .preTags(DEFAULT_PRE_TAGS).postTags(DEFAULT_POST_TAGS).scoreOrdered(DEFAULT_SCORE_ORDERED).highlightFilter(DEFAULT_HIGHLIGHT_FILTER)
                .requireFieldMatch(DEFAULT_REQUIRE_FIELD_MATCH).forceSource(DEFAULT_FORCE_SOURCE).fragmentCharSize(DEFAULT_FRAGMENT_CHAR_SIZE).numberOfFragments(DEFAULT_NUMBER_OF_FRAGMENTS)
                .encoder(DEFAULT_ENCODER).boundaryMaxScan(SimpleBoundaryScanner.DEFAULT_MAX_SCAN)
                .boundaryChars(SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS)
                .noMatchSize(DEFAULT_NO_MATCH_SIZE).phraseLimit(DEFAULT_PHRASE_LIMIT);
    }

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
            preTags(DEFAULT_PRE_TAGS);
            postTags(DEFAULT_POST_TAGS);
            break;
        case "styled":
            preTags(DEFAULT_STYLED_PRE_TAG);
            postTags(DEFAULT_STYLED_POST_TAGS);
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
        builder.startObject(HIGHLIGHT_ELEMENT_NAME);
        innerXContent(builder);
        builder.endObject();
        return builder;
    }

    /**
     * parse options only present in top level highlight builder (`tags_schema`, `encoder` and nested `fields`)
     */
    @Override
    protected boolean doFromXContent(QueryParseContext parseContext, String currentFieldName, Token currentToken) throws IOException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token;
        boolean foundCurrentFieldMatch = false;
        if (currentToken.isValue()) {
            if (parseContext.parseFieldMatcher().match(currentFieldName, TAGS_SCHEMA_FIELD)) {
                tagsSchema(parser.text());
                foundCurrentFieldMatch = true;
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, ENCODER_FIELD)) {
                encoder(parser.text());
                foundCurrentFieldMatch = true;
            }
        } else if (currentToken == Token.START_ARRAY && parseContext.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
            useExplicitFieldOrder(true);
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            field(HighlightBuilder.Field.PROTOTYPE.fromXContent(parseContext));
                        }
                    }
                    foundCurrentFieldMatch = true;
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "If highlighter fields is an array it must contain objects containing a single field");
                }
            }
        } else if (currentToken == Token.START_OBJECT && parseContext.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field(HighlightBuilder.Field.PROTOTYPE.fromXContent(parseContext));
                }
            }
            foundCurrentFieldMatch = true;
        }
        return foundCurrentFieldMatch;
    }

    public SearchContextHighlight build(QueryShardContext context) throws IOException {
        // create template global options that are later merged with any partial field options
        final SearchContextHighlight.FieldOptions.Builder globalOptionsBuilder = new SearchContextHighlight.FieldOptions.Builder();
        globalOptionsBuilder.encoder(this.encoder);
        transferOptions(this, globalOptionsBuilder, context);

        // overwrite unset global options by default values
        globalOptionsBuilder.merge(defaultFieldOptions().build());

        // create field options
        Collection<org.elasticsearch.search.highlight.SearchContextHighlight.Field> fieldOptions = new ArrayList<>();
        for (Field field : this.fields) {
            final SearchContextHighlight.FieldOptions.Builder fieldOptionsBuilder = new SearchContextHighlight.FieldOptions.Builder();
            fieldOptionsBuilder.fragmentOffset(field.fragmentOffset);
            if (field.matchedFields != null) {
                Set<String> matchedFields = new HashSet<String>(field.matchedFields.length);
                Collections.addAll(matchedFields, field.matchedFields);
                fieldOptionsBuilder.matchedFields(matchedFields);
            }
            transferOptions(field, fieldOptionsBuilder, context);
            fieldOptions.add(new SearchContextHighlight.Field(field.name(), fieldOptionsBuilder.merge(globalOptionsBuilder.build()).build()));
        }
        return new SearchContextHighlight(fieldOptions);
    }

    /**
     * Transfers field options present in the input {@link AbstractHighlighterBuilder} to the receiving
     * {@link FieldOptions.Builder}, effectively overwriting existing settings
     * @param targetOptionsBuilder the receiving options builder
     * @param highlighterBuilder highlight builder with the input options
     * @param context needed to convert {@link QueryBuilder} to {@link Query}
     * @throws IOException on errors parsing any optional nested highlight query
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void transferOptions(AbstractHighlighterBuilder highlighterBuilder, SearchContextHighlight.FieldOptions.Builder targetOptionsBuilder, QueryShardContext context) throws IOException {
        if (highlighterBuilder.preTags != null) {
            targetOptionsBuilder.preTags(highlighterBuilder.preTags);
        }
        if (highlighterBuilder.postTags != null) {
            targetOptionsBuilder.postTags(highlighterBuilder.postTags);
        }
        if (highlighterBuilder.order != null) {
            targetOptionsBuilder.scoreOrdered(highlighterBuilder.order == Order.SCORE);
        }
        if (highlighterBuilder.highlightFilter != null) {
            targetOptionsBuilder.highlightFilter(highlighterBuilder.highlightFilter);
        }
        if (highlighterBuilder.fragmentSize != null) {
            targetOptionsBuilder.fragmentCharSize(highlighterBuilder.fragmentSize);
        }
        if (highlighterBuilder.numOfFragments != null) {
            targetOptionsBuilder.numberOfFragments(highlighterBuilder.numOfFragments);
        }
        if (highlighterBuilder.requireFieldMatch != null) {
            targetOptionsBuilder.requireFieldMatch(highlighterBuilder.requireFieldMatch);
        }
        if (highlighterBuilder.boundaryMaxScan != null) {
            targetOptionsBuilder.boundaryMaxScan(highlighterBuilder.boundaryMaxScan);
        }
        if (highlighterBuilder.boundaryChars != null) {
            targetOptionsBuilder.boundaryChars(convertCharArray(highlighterBuilder.boundaryChars));
        }
        if (highlighterBuilder.highlighterType != null) {
            targetOptionsBuilder.highlighterType(highlighterBuilder.highlighterType);
        }
        if (highlighterBuilder.fragmenter != null) {
            targetOptionsBuilder.fragmenter(highlighterBuilder.fragmenter);
        }
        if (highlighterBuilder.noMatchSize != null) {
            targetOptionsBuilder.noMatchSize(highlighterBuilder.noMatchSize);
        }
        if (highlighterBuilder.forceSource != null) {
            targetOptionsBuilder.forceSource(highlighterBuilder.forceSource);
        }
        if (highlighterBuilder.phraseLimit != null) {
            targetOptionsBuilder.phraseLimit(highlighterBuilder.phraseLimit);
        }
        if (highlighterBuilder.options != null) {
            targetOptionsBuilder.options(highlighterBuilder.options);
        }
        if (highlighterBuilder.highlightQuery != null) {
            targetOptionsBuilder.highlightQuery(highlighterBuilder.highlightQuery.toQuery(context));
        }
    }

    private static Character[] convertCharArray(char[] array) {
        if (array == null) {
            return null;
        }
        Character[] charArray = new Character[array.length];
        for (int i = 0; i < array.length; i++) {
            charArray[i] = array[i];
        }
        return charArray;
    }

    public void innerXContent(XContentBuilder builder) throws IOException {
        // first write common options
        commonOptionsToXContent(builder);
        // special options for top-level highlighter
        if (encoder != null) {
            builder.field(ENCODER_FIELD.getPreferredName(), encoder);
        }
        if (fields.size() > 0) {
            if (useExplicitFieldOrder) {
                builder.startArray(FIELDS_FIELD.getPreferredName());
            } else {
                builder.startObject(FIELDS_FIELD.getPreferredName());
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
    protected HighlightBuilder createInstance(XContentParser parser) {
        return new HighlightBuilder();
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
                builder.field(FRAGMENT_OFFSET_FIELD.getPreferredName(), fragmentOffset);
            }
            if (matchedFields != null) {
                builder.field(MATCHED_FIELDS_FIELD.getPreferredName(), matchedFields);
            }
            builder.endObject();
        }

        /**
         * parse options only present in field highlight builder (`fragment_offset`, `matched_fields`)
         */
        @Override
        protected boolean doFromXContent(QueryParseContext parseContext, String currentFieldName, Token currentToken) throws IOException {
            XContentParser parser = parseContext.parser();
            boolean foundCurrentFieldMatch = false;
            if (parseContext.parseFieldMatcher().match(currentFieldName, FRAGMENT_OFFSET_FIELD) && currentToken.isValue()) {
                fragmentOffset(parser.intValue());
                foundCurrentFieldMatch = true;
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, MATCHED_FIELDS_FIELD)
                    && currentToken == XContentParser.Token.START_ARRAY) {
                List<String> matchedFields = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    matchedFields.add(parser.text());
                }
                matchedFields(matchedFields.toArray(new String[matchedFields.size()]));
                foundCurrentFieldMatch = true;
            }
            return foundCurrentFieldMatch;
        }

        @Override
        protected Field createInstance(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String fieldname = parser.currentName();
                return new Field(fieldname);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unknown token type [{}], expected field name", parser.currentToken());
            }
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

    public enum Order implements Writeable<Order> {
        NONE, SCORE;

        static Order PROTOTYPE = NONE;

        @Override
        public Order readFrom(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Order ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal());
        }

        public static Order fromString(String order) {
            if (order.toUpperCase(Locale.ROOT).equals(SCORE.name())) {
                return Order.SCORE;
            }
            return NONE;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
