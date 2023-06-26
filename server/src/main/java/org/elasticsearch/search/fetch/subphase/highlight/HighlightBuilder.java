/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext.FieldOptions;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.xcontent.ObjectParser.fromList;

/**
 * A builder for search highlighting. Settings can control how large fields
 * are summarized to show only selected snippets ("fragments") containing search terms.
 *
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#highlight()
 */
public class HighlightBuilder extends AbstractHighlighterBuilder<HighlightBuilder> {
    /** default for whether to highlight fields based on the source even if stored separately */
    public static final boolean DEFAULT_FORCE_SOURCE = false;
    /** default for whether a field should be highlighted only if a query matches that field */
    public static final boolean DEFAULT_REQUIRE_FIELD_MATCH = true;
    /** default for whether to stop highlighting at the defined max_analyzed_offset to avoid exceptions for longer texts */
    public static final Integer DEFAULT_MAX_ANALYZED_OFFSET = null;
    /** default for whether {@code fvh} should provide highlighting on filter clauses */
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
    static final String[] DEFAULT_PRE_TAGS = new String[] { "<em>" };
    /** the default closing tag  */
    static final String[] DEFAULT_POST_TAGS = new String[] { "</em>" };

    /** the default opening tags when {@code tag_schema = "styled"}  */
    public static final String[] DEFAULT_STYLED_PRE_TAG = {
        "<em class=\"hlt1\">",
        "<em class=\"hlt2\">",
        "<em class=\"hlt3\">",
        "<em class=\"hlt4\">",
        "<em class=\"hlt5\">",
        "<em class=\"hlt6\">",
        "<em class=\"hlt7\">",
        "<em class=\"hlt8\">",
        "<em class=\"hlt9\">",
        "<em class=\"hlt10\">" };
    /** the default closing tags when {@code tag_schema = "styled"}  */
    public static final String[] DEFAULT_STYLED_POST_TAGS = { "</em>" };

    /**
     * a {@link FieldOptions} with default settings
     */
    static final FieldOptions defaultOptions = new SearchHighlightContext.FieldOptions.Builder().preTags(DEFAULT_PRE_TAGS)
        .postTags(DEFAULT_POST_TAGS)
        .scoreOrdered(DEFAULT_SCORE_ORDERED)
        .highlightFilter(DEFAULT_HIGHLIGHT_FILTER)
        .requireFieldMatch(DEFAULT_REQUIRE_FIELD_MATCH)
        .maxAnalyzedOffset(DEFAULT_MAX_ANALYZED_OFFSET)
        .fragmentCharSize(DEFAULT_FRAGMENT_CHAR_SIZE)
        .numberOfFragments(DEFAULT_NUMBER_OF_FRAGMENTS)
        .encoder(DEFAULT_ENCODER)
        .boundaryMaxScan(SimpleBoundaryScanner.DEFAULT_MAX_SCAN)
        .boundaryChars(SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS)
        .boundaryScannerLocale(Locale.ROOT)
        .noMatchSize(DEFAULT_NO_MATCH_SIZE)
        .phraseLimit(DEFAULT_PHRASE_LIMIT)
        .build();

    private final List<Field> fields;

    private String encoder;

    private boolean useExplicitFieldOrder = false;

    public HighlightBuilder() {
        fields = new ArrayList<>();
    }

    public HighlightBuilder(HighlightBuilder template, QueryBuilder highlightQuery, List<Field> fields) {
        super(template, highlightQuery);
        this.encoder = template.encoder;
        this.useExplicitFieldOrder = template.useExplicitFieldOrder;
        this.fields = fields;
    }

    /**
     * Read from a stream.
     */
    public HighlightBuilder(StreamInput in) throws IOException {
        super(in);
        encoder(in.readOptionalString());
        useExplicitFieldOrder(in.readBoolean());
        this.fields = in.readList(Field::new);
        assert this.equals(new HighlightBuilder(this, highlightQuery, fields)) : "copy constructor is broken";
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(encoder);
        out.writeBoolean(useExplicitFieldOrder);
        out.writeList(fields);
    }

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
        return field(new Field(name).fragmentSize(fragmentSize).numOfFragments(numberOfFragments).fragmentOffset(fragmentOffset));
    }

    public HighlightBuilder field(Field field) {
        fields.add(field);
        return this;
    }

    void fields(List<Field> fields) {
        this.fields.addAll(fields);
    }

    public List<Field> fields() {
        return this.fields;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allowed schemes
     * are {@code styled} and {@code default}.
     *
     * @param schemaName The tag scheme name
     */
    public HighlightBuilder tagsSchema(String schemaName) {
        switch (schemaName) {
            case "default" -> {
                preTags(DEFAULT_PRE_TAGS);
                postTags(DEFAULT_POST_TAGS);
            }
            case "styled" -> {
                preTags(DEFAULT_STYLED_PRE_TAG);
                postTags(DEFAULT_STYLED_POST_TAGS);
            }
            default -> throw new IllegalArgumentException("Unknown tag schema [" + schemaName + "]");
        }
        return this;
    }

    /**
     * Set encoder for the highlighting
     * are {@code html} and {@code default}.
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
        builder.startObject();
        innerXContent(builder);
        builder.endObject();
        return builder;
    }

    private static final BiFunction<XContentParser, HighlightBuilder, HighlightBuilder> PARSER;
    static {
        ObjectParser<HighlightBuilder, Void> parser = new ObjectParser<>("highlight");
        parser.declareString(HighlightBuilder::tagsSchema, new ParseField("tags_schema"));
        parser.declareString(HighlightBuilder::encoder, ENCODER_FIELD);
        parser.declareNamedObjects(
            HighlightBuilder::fields,
            Field.PARSER,
            (HighlightBuilder hb) -> hb.useExplicitFieldOrder(true),
            FIELDS_FIELD
        );
        PARSER = setupParser(parser);
    }

    public static HighlightBuilder fromXContent(XContentParser p) {
        return PARSER.apply(p, new HighlightBuilder());
    }

    public SearchHighlightContext build(SearchExecutionContext context, QueryBuilder originalQuery) throws IOException {
        // create template global options that are later merged with any partial field options
        final SearchHighlightContext.FieldOptions.Builder globalOptionsBuilder = new SearchHighlightContext.FieldOptions.Builder();
        globalOptionsBuilder.encoder(this.encoder);
        transferOptions(this, globalOptionsBuilder, context);

        // overwrite unset global options by default values
        globalOptionsBuilder.merge(defaultOptions);

        // create field options
        Collection<SearchHighlightContext.Field> fieldOptions = new ArrayList<>();
        for (Field field : this.fields) {
            final SearchHighlightContext.FieldOptions.Builder fieldOptionsBuilder = new SearchHighlightContext.FieldOptions.Builder();
            fieldOptionsBuilder.fragmentOffset(field.fragmentOffset);
            if (field.matchedFields != null) {
                Set<String> matchedFields = Sets.newHashSetWithExpectedSize(field.matchedFields.length);
                Collections.addAll(matchedFields, field.matchedFields);
                fieldOptionsBuilder.matchedFields(matchedFields);
            }
            transferOptions(field, fieldOptionsBuilder, context);
            fieldOptions.add(
                new SearchHighlightContext.Field(field.name(), fieldOptionsBuilder.merge(globalOptionsBuilder.build()).build())
            );
        }
        return new SearchHighlightContext(fieldOptions, originalQuery);
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
    private static void transferOptions(
        AbstractHighlighterBuilder highlighterBuilder,
        SearchHighlightContext.FieldOptions.Builder targetOptionsBuilder,
        SearchExecutionContext context
    ) throws IOException {
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
        if (highlighterBuilder.maxAnalyzedOffset != null) {
            targetOptionsBuilder.maxAnalyzedOffset(highlighterBuilder.maxAnalyzedOffset);
        }
        if (highlighterBuilder.boundaryScannerType != null) {
            targetOptionsBuilder.boundaryScannerType(highlighterBuilder.boundaryScannerType);
        }
        if (highlighterBuilder.boundaryMaxScan != null) {
            targetOptionsBuilder.boundaryMaxScan(highlighterBuilder.boundaryMaxScan);
        }
        if (highlighterBuilder.boundaryChars != null) {
            targetOptionsBuilder.boundaryChars(convertCharArray(highlighterBuilder.boundaryChars));
        }
        if (highlighterBuilder.boundaryScannerLocale != null) {
            targetOptionsBuilder.boundaryScannerLocale(highlighterBuilder.boundaryScannerLocale);
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

    static Character[] convertCharArray(char[] array) {
        if (array == null) {
            return null;
        }
        Character[] charArray = new Character[array.length];
        for (int i = 0; i < array.length; i++) {
            charArray[i] = array[i];
        }
        return charArray;
    }

    @Override
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
    protected int doHashCode() {
        return Objects.hash(encoder, useExplicitFieldOrder, fields);
    }

    @Override
    protected boolean doEquals(HighlightBuilder other) {
        return Objects.equals(encoder, other.encoder)
            && Objects.equals(useExplicitFieldOrder, other.useExplicitFieldOrder)
            && Objects.equals(fields, other.fields);
    }

    @Override
    public HighlightBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder highlightQuery = this.highlightQuery;
        if (highlightQuery != null) {
            highlightQuery = this.highlightQuery.rewrite(ctx);
        }
        List<Field> fields = Rewriteable.rewrite(this.fields, ctx);
        if (highlightQuery == this.highlightQuery && fields == this.fields) {
            return this;
        }
        return new HighlightBuilder(this, highlightQuery, fields);

    }

    public static class Field extends AbstractHighlighterBuilder<Field> {
        static final NamedObjectParser<Field, Void> PARSER;
        static {
            ObjectParser<Field, Void> parser = new ObjectParser<>("highlight_field");
            parser.declareInt(Field::fragmentOffset, FRAGMENT_OFFSET_FIELD);
            parser.declareStringArray(fromList(String.class, Field::matchedFields), MATCHED_FIELDS_FIELD);
            BiFunction<XContentParser, Field, Field> decoratedParser = setupParser(parser);
            PARSER = (XContentParser p, Void c, String name) -> decoratedParser.apply(p, new Field(name));
        }

        private final String name;

        int fragmentOffset = -1;

        String[] matchedFields;

        public Field(String name) {
            this.name = name;
        }

        private Field(Field template, QueryBuilder builder) {
            super(template, builder);
            name = template.name;
            fragmentOffset = template.fragmentOffset;
            matchedFields = template.matchedFields;
        }

        /**
         * Read from a stream.
         */
        public Field(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            fragmentOffset(in.readVInt());
            matchedFields(in.readOptionalStringArray());
            assert this.equals(new Field(this, highlightQuery)) : "copy constructor is broken";
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(fragmentOffset);
            out.writeOptionalStringArray(matchedFields);
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

        @Override
        public void innerXContent(XContentBuilder builder) throws IOException {
            builder.startObject(name);
            // write common options
            commonOptionsToXContent(builder);
            // write special field-highlighter options
            if (fragmentOffset != -1) {
                builder.field(FRAGMENT_OFFSET_FIELD.getPreferredName(), fragmentOffset);
            }
            if (matchedFields != null) {
                builder.array(MATCHED_FIELDS_FIELD.getPreferredName(), matchedFields);
            }
            builder.endObject();
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(name, fragmentOffset, Arrays.hashCode(matchedFields));
        }

        @Override
        protected boolean doEquals(Field other) {
            return Objects.equals(name, other.name)
                && Objects.equals(fragmentOffset, other.fragmentOffset)
                && Arrays.equals(matchedFields, other.matchedFields);
        }

        @Override
        public Field rewrite(QueryRewriteContext ctx) throws IOException {
            if (highlightQuery != null) {
                QueryBuilder rewrite = highlightQuery.rewrite(ctx);
                if (rewrite != highlightQuery) {
                    return new Field(this, rewrite);
                }
            }
            return this;
        }
    }

    public enum Order implements Writeable {
        NONE,
        SCORE;

        public static Order readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Order.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
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

    public enum BoundaryScannerType implements Writeable {
        CHARS,
        WORD,
        SENTENCE;

        public static BoundaryScannerType readFromStream(StreamInput in) throws IOException {
            return in.readEnum(BoundaryScannerType.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static BoundaryScannerType fromString(String boundaryScannerType) {
            return valueOf(boundaryScannerType.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
