/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.BoundaryScannerType;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Order;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.xcontent.ObjectParser.fromList;

/**
 * This abstract class holds parameters shared by {@link HighlightBuilder} and {@link HighlightBuilder.Field}
 * and provides the common setters, equality, hashCode calculation and common serialization
 */
public abstract class AbstractHighlighterBuilder<HB extends AbstractHighlighterBuilder<?>>
    implements
        Writeable,
        Rewriteable<HB>,
        ToXContentObject {
    public static final ParseField PRE_TAGS_FIELD = new ParseField("pre_tags");
    public static final ParseField POST_TAGS_FIELD = new ParseField("post_tags");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField HIGHLIGHT_FILTER_FIELD = new ParseField("highlight_filter");
    public static final ParseField FRAGMENT_SIZE_FIELD = new ParseField("fragment_size");
    public static final ParseField FRAGMENT_OFFSET_FIELD = new ParseField("fragment_offset");
    public static final ParseField NUMBER_OF_FRAGMENTS_FIELD = new ParseField("number_of_fragments");
    public static final ParseField ENCODER_FIELD = new ParseField("encoder");
    public static final ParseField TAGS_SCHEMA_FIELD = new ParseField("tags_schema");
    public static final ParseField REQUIRE_FIELD_MATCH_FIELD = new ParseField("require_field_match");
    public static final ParseField BOUNDARY_SCANNER_FIELD = new ParseField("boundary_scanner");
    public static final ParseField BOUNDARY_MAX_SCAN_FIELD = new ParseField("boundary_max_scan");
    public static final ParseField BOUNDARY_CHARS_FIELD = new ParseField("boundary_chars");
    public static final ParseField BOUNDARY_SCANNER_LOCALE_FIELD = new ParseField("boundary_scanner_locale");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FRAGMENTER_FIELD = new ParseField("fragmenter");
    public static final ParseField NO_MATCH_SIZE_FIELD = new ParseField("no_match_size");
    public static final ParseField FORCE_SOURCE_FIELD = new ParseField("force_source").withAllDeprecated();
    public static final ParseField PHRASE_LIMIT_FIELD = new ParseField("phrase_limit");
    public static final ParseField OPTIONS_FIELD = new ParseField("options");
    public static final ParseField HIGHLIGHT_QUERY_FIELD = new ParseField("highlight_query");
    public static final ParseField MATCHED_FIELDS_FIELD = new ParseField("matched_fields");
    public static final ParseField MAX_ANALYZED_OFFSET_FIELD = new ParseField("max_analyzed_offset");

    protected String encoder;

    protected String[] preTags;

    protected String[] postTags;

    protected Integer fragmentSize;

    protected Integer numOfFragments;

    protected String highlighterType;

    protected String fragmenter;

    protected QueryBuilder highlightQuery;

    protected Order order;

    protected Boolean highlightFilter;

    protected BoundaryScannerType boundaryScannerType;

    protected Integer boundaryMaxScan;

    protected char[] boundaryChars;

    protected Locale boundaryScannerLocale;

    protected Integer noMatchSize;

    protected Integer phraseLimit;

    protected Map<String, Object> options;

    protected Boolean requireFieldMatch;

    protected Integer maxAnalyzedOffset;

    public AbstractHighlighterBuilder() {}

    protected AbstractHighlighterBuilder(AbstractHighlighterBuilder<?> template, QueryBuilder queryBuilder) {
        preTags = template.preTags;
        postTags = template.postTags;
        fragmentSize = template.fragmentSize;
        numOfFragments = template.numOfFragments;
        encoder = template.encoder;
        highlighterType = template.highlighterType;
        fragmenter = template.fragmenter;
        highlightQuery = queryBuilder;
        order = template.order;
        highlightFilter = template.highlightFilter;
        boundaryScannerType = template.boundaryScannerType;
        boundaryMaxScan = template.boundaryMaxScan;
        boundaryChars = template.boundaryChars;
        boundaryScannerLocale = template.boundaryScannerLocale;
        noMatchSize = template.noMatchSize;
        phraseLimit = template.phraseLimit;
        options = template.options;
        requireFieldMatch = template.requireFieldMatch;
        this.maxAnalyzedOffset = template.maxAnalyzedOffset;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("this-escape")
    protected AbstractHighlighterBuilder(StreamInput in) throws IOException {
        preTags(in.readOptionalStringArray());
        postTags(in.readOptionalStringArray());
        fragmentSize(in.readOptionalVInt());
        numOfFragments(in.readOptionalVInt());
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            encoder(in.readOptionalString());
        }
        highlighterType(in.readOptionalString());
        fragmenter(in.readOptionalString());
        if (in.readBoolean()) {
            highlightQuery(in.readNamedWriteable(QueryBuilder.class));
        }
        order(in.readOptionalWriteable(Order::readFromStream));
        highlightFilter(in.readOptionalBoolean());
        if (in.getTransportVersion().before(TransportVersions.V_8_8_0)) {
            in.readOptionalBoolean();   // force_source, now deprecated
        }
        boundaryScannerType(in.readOptionalWriteable(BoundaryScannerType::readFromStream));
        boundaryMaxScan(in.readOptionalVInt());
        if (in.readBoolean()) {
            boundaryChars(in.readString().toCharArray());
        }
        if (in.readBoolean()) {
            boundaryScannerLocale(in.readString());
        }
        noMatchSize(in.readOptionalVInt());
        phraseLimit(in.readOptionalVInt());
        if (in.readBoolean()) {
            options(in.readGenericMap());
        }
        requireFieldMatch(in.readOptionalBoolean());
        maxAnalyzedOffset(in.readOptionalInt());
    }

    /**
     * write common parameters to {@link StreamOutput}
     */
    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(preTags);
        out.writeOptionalStringArray(postTags);
        out.writeOptionalVInt(fragmentSize);
        out.writeOptionalVInt(numOfFragments);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalString(encoder);
        }
        out.writeOptionalString(highlighterType);
        out.writeOptionalString(fragmenter);
        boolean hasQuery = highlightQuery != null;
        out.writeBoolean(hasQuery);
        if (hasQuery) {
            out.writeNamedWriteable(highlightQuery);
        }
        out.writeOptionalWriteable(order);
        out.writeOptionalBoolean(highlightFilter);
        if (out.getTransportVersion().before(TransportVersions.V_8_8_0)) {
            out.writeOptionalBoolean(false);
        }
        out.writeOptionalWriteable(boundaryScannerType);
        out.writeOptionalVInt(boundaryMaxScan);
        boolean hasBounaryChars = boundaryChars != null;
        out.writeBoolean(hasBounaryChars);
        if (hasBounaryChars) {
            out.writeString(String.valueOf(boundaryChars));
        }
        boolean hasBoundaryScannerLocale = boundaryScannerLocale != null;
        out.writeBoolean(hasBoundaryScannerLocale);
        if (hasBoundaryScannerLocale) {
            out.writeString(boundaryScannerLocale.toLanguageTag());
        }
        out.writeOptionalVInt(noMatchSize);
        out.writeOptionalVInt(phraseLimit);
        boolean hasOptions = options != null;
        out.writeBoolean(hasOptions);
        if (hasOptions) {
            out.writeGenericMap(options);
        }
        out.writeOptionalBoolean(requireFieldMatch);
        out.writeOptionalInt(maxAnalyzedOffset);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * Set the pre tags that will be used for highlighting.
     */
    @SuppressWarnings("unchecked")
    public HB preTags(String... preTags) {
        this.preTags = preTags;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #preTags(String...)}
     */
    public String[] preTags() {
        return this.preTags;
    }

    /**
     * Set the post tags that will be used for highlighting.
     */
    @SuppressWarnings("unchecked")
    public HB postTags(String... postTags) {
        this.postTags = postTags;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #postTags(String...)}
     */
    public String[] postTags() {
        return this.postTags;
    }

    /**
     * Set the fragment size in characters, defaults to {@link HighlightBuilder#DEFAULT_FRAGMENT_CHAR_SIZE}
     */
    @SuppressWarnings("unchecked")
    public HB fragmentSize(Integer fragmentSize) {
        this.fragmentSize = fragmentSize;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #fragmentSize(Integer)}
     */
    public Integer fragmentSize() {
        return this.fragmentSize;
    }

    /**
     * Set the number of fragments, defaults to {@link HighlightBuilder#DEFAULT_NUMBER_OF_FRAGMENTS}
     */
    @SuppressWarnings("unchecked")
    public HB numOfFragments(Integer numOfFragments) {
        this.numOfFragments = numOfFragments;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #numOfFragments(Integer)}
     */
    public Integer numOfFragments() {
        return this.numOfFragments;
    }

    /**
     * Set the encoder, defaults to {@link HighlightBuilder#DEFAULT_ENCODER}
     */
    @SuppressWarnings("unchecked")
    public HB encoder(String encoder) {
        this.encoder = encoder;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #encoder(String)}
     */
    public String encoder() {
        return this.encoder;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allowed schemes
     * are {@code styled} and {@code default}.
     *
     * @param schemaName The tag scheme name
     */
    @SuppressWarnings("unchecked")
    public HB tagsSchema(String schemaName) {
        switch (schemaName) {
            case "default" -> {
                preTags(HighlightBuilder.DEFAULT_PRE_TAGS);
                postTags(HighlightBuilder.DEFAULT_POST_TAGS);
            }
            case "styled" -> {
                preTags(HighlightBuilder.DEFAULT_STYLED_PRE_TAG);
                postTags(HighlightBuilder.DEFAULT_STYLED_POST_TAGS);
            }
            default -> throw new IllegalArgumentException("Unknown tag schema [" + schemaName + "]");
        }
        return (HB) this;
    }

    /**
     * Set type of highlighter to use. Out of the box supported types
     * are {@code unified}, {@code plain} and {@code fvh}.
     * Defaults to {@code unified}.
     * Details of the different highlighter types are covered in the reference guide.
     */
    @SuppressWarnings("unchecked")
    public HB highlighterType(String highlighterType) {
        this.highlighterType = highlighterType;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #highlighterType(String)}
     */
    public String highlighterType() {
        return this.highlighterType;
    }

    /**
     * Sets what fragmenter to use to break up text that is eligible for highlighting.
     * This option is only applicable when using the plain highlighterType {@code highlighter}.
     * Permitted values are "simple" or "span" relating to {@link SimpleFragmenter} and
     * {@link SimpleSpanFragmenter} implementations respectively with the default being "span"
     */
    @SuppressWarnings("unchecked")
    public HB fragmenter(String fragmenter) {
        this.fragmenter = fragmenter;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #fragmenter(String)}
     */
    public String fragmenter() {
        return this.fragmenter;
    }

    /**
     * Sets a query to be used for highlighting instead of the search query.
     */
    @SuppressWarnings("unchecked")
    public HB highlightQuery(QueryBuilder highlightQuery) {
        this.highlightQuery = highlightQuery;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #highlightQuery(QueryBuilder)}
     */
    public QueryBuilder highlightQuery() {
        return this.highlightQuery;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be {@code score}, which then it will be ordered
     * by score of the fragments, or {@code none}.
     */
    public HB order(String order) {
        return order(Order.fromString(order));
    }

    /**
     * By default, fragments of a field are ordered by the order in the highlighted text.
     * If set to {@link Order#SCORE}, this changes order to score of the fragments.
     */
    @SuppressWarnings("unchecked")
    public HB order(Order scoreOrdered) {
        this.order = scoreOrdered;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #order(Order)}
     */
    public Order order() {
        return this.order;
    }

    /**
     * Set this to true when using the highlighterType {@code fvh}
     * and you want to provide highlighting on filter clauses in your
     * query. Default is {@code false}.
     */
    @SuppressWarnings("unchecked")
    public HB highlightFilter(Boolean highlightFilter) {
        this.highlightFilter = highlightFilter;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #highlightFilter(Boolean)}
     */
    public Boolean highlightFilter() {
        return this.highlightFilter;
    }

    /**
     * When using the highlighterType {@code fvh} this setting
     * controls which scanner to use for fragment boundaries, and defaults to "simple".
     */
    @SuppressWarnings("unchecked")
    public HB boundaryScannerType(String boundaryScannerType) {
        this.boundaryScannerType = BoundaryScannerType.fromString(boundaryScannerType);
        return (HB) this;
    }

    /**
     * When using the highlighterType {@code fvh} this setting
     * controls which scanner to use for fragment boundaries, and defaults to "simple".
     */
    @SuppressWarnings("unchecked")
    public HB boundaryScannerType(BoundaryScannerType boundaryScannerType) {
        this.boundaryScannerType = boundaryScannerType;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #boundaryScannerType(String)}
     */
    public BoundaryScannerType boundaryScannerType() {
        return this.boundaryScannerType;
    }

    /**
     * When using the highlighterType {@code fvh} this setting
     * controls how far to look for boundary characters, and defaults to 20.
     */
    @SuppressWarnings("unchecked")
    public HB boundaryMaxScan(Integer boundaryMaxScan) {
        this.boundaryMaxScan = boundaryMaxScan;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #boundaryMaxScan(Integer)}
     */
    public Integer boundaryMaxScan() {
        return this.boundaryMaxScan;
    }

    /**
     * When using the highlighterType {@code fvh} this setting
     * defines what constitutes a boundary for highlighting. It’s a single string with
     * each boundary character defined in it. It defaults to .,!? \t\n
     */
    @SuppressWarnings("unchecked")
    public HB boundaryChars(char[] boundaryChars) {
        this.boundaryChars = boundaryChars;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #boundaryChars(char[])}
     */
    public char[] boundaryChars() {
        return this.boundaryChars;
    }

    /**
     * When using the highlighterType {@code fvh} and boundaryScannerType {@code break_iterator}, this setting
     * controls the locale to use by the BreakIterator, defaults to "root".
     */
    @SuppressWarnings("unchecked")
    public HB boundaryScannerLocale(String boundaryScannerLocale) {
        if (boundaryScannerLocale != null) {
            this.boundaryScannerLocale = Locale.forLanguageTag(boundaryScannerLocale);
        }
        return (HB) this;
    }

    /**
     * Allows to set custom options for custom highlighters.
     */
    @SuppressWarnings("unchecked")
    public HB options(Map<String, Object> options) {
        this.options = options;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #options(Map)}
     */
    public Map<String, Object> options() {
        return this.options;
    }

    /**
     * Set to true to cause a field to be highlighted only if a query matches that field.
     * Default is false meaning that terms are highlighted on all requested fields regardless
     * if the query matches specifically on them.
     */
    @SuppressWarnings("unchecked")
    public HB requireFieldMatch(Boolean requireFieldMatch) {
        this.requireFieldMatch = requireFieldMatch;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #requireFieldMatch(Boolean)}
     */
    public Boolean requireFieldMatch() {
        return this.requireFieldMatch;
    }

    /**
     * Sets the size of the fragment to return from the beginning of the field if there are no matches to
     * highlight and the field doesn't also define noMatchSize.
     * @param noMatchSize integer to set or null to leave out of request.  default is null.
     * @return this for chaining
     */
    @SuppressWarnings("unchecked")
    public HB noMatchSize(Integer noMatchSize) {
        this.noMatchSize = noMatchSize;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #noMatchSize(Integer)}
     */
    public Integer noMatchSize() {
        return this.noMatchSize;
    }

    /**
     * Sets the maximum number of phrases the fvh will consider if the field doesn't also define phraseLimit.
     * @param phraseLimit maximum number of phrases the fvh will consider
     * @return this for chaining
     */
    @SuppressWarnings("unchecked")
    public HB phraseLimit(Integer phraseLimit) {
        this.phraseLimit = phraseLimit;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #phraseLimit(Integer)}
     */
    public Integer phraseLimit() {
        return this.phraseLimit;
    }

    /**
     * Set to a non-negative value which represents the max offset used to analyze
     * the field thus avoiding exceptions if the field exceeds this limit.
     */
    @SuppressWarnings("unchecked")
    public HB maxAnalyzedOffset(Integer maxAnalyzedOffset) {
        if (maxAnalyzedOffset != null && maxAnalyzedOffset <= 0) {
            throw new IllegalArgumentException("[" + MAX_ANALYZED_OFFSET_FIELD + "] must be a positive integer");
        }
        this.maxAnalyzedOffset = maxAnalyzedOffset;
        return (HB) this;
    }

    /**
     * @return the value set by {@link #maxAnalyzedOffset(Integer)}
     */
    public Integer maxAnalyzedOffset() {
        return this.maxAnalyzedOffset;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerXContent(builder);
        builder.endObject();
        return builder;
    }

    protected abstract void innerXContent(XContentBuilder builder) throws IOException;

    void commonOptionsToXContent(XContentBuilder builder) throws IOException {
        if (preTags != null) {
            builder.array(PRE_TAGS_FIELD.getPreferredName(), preTags);
        }
        if (postTags != null) {
            builder.array(POST_TAGS_FIELD.getPreferredName(), postTags);
        }
        if (fragmentSize != null) {
            builder.field(FRAGMENT_SIZE_FIELD.getPreferredName(), fragmentSize);
        }
        if (numOfFragments != null) {
            builder.field(NUMBER_OF_FRAGMENTS_FIELD.getPreferredName(), numOfFragments);
        }
        if (encoder != null) {
            builder.field(ENCODER_FIELD.getPreferredName(), encoder);
        }
        if (highlighterType != null) {
            builder.field(TYPE_FIELD.getPreferredName(), highlighterType);
        }
        if (fragmenter != null) {
            builder.field(FRAGMENTER_FIELD.getPreferredName(), fragmenter);
        }
        if (highlightQuery != null) {
            builder.field(HIGHLIGHT_QUERY_FIELD.getPreferredName(), highlightQuery);
        }
        if (order != null) {
            builder.field(ORDER_FIELD.getPreferredName(), order.toString());
        }
        if (highlightFilter != null) {
            builder.field(HIGHLIGHT_FILTER_FIELD.getPreferredName(), highlightFilter);
        }
        if (boundaryScannerType != null) {
            builder.field(BOUNDARY_SCANNER_FIELD.getPreferredName(), boundaryScannerType.name());
        }
        if (boundaryMaxScan != null) {
            builder.field(BOUNDARY_MAX_SCAN_FIELD.getPreferredName(), boundaryMaxScan);
        }
        if (boundaryChars != null) {
            builder.field(BOUNDARY_CHARS_FIELD.getPreferredName(), new String(boundaryChars));
        }
        if (boundaryScannerLocale != null) {
            builder.field(BOUNDARY_SCANNER_LOCALE_FIELD.getPreferredName(), boundaryScannerLocale.toLanguageTag());
        }
        if (options != null && options.size() > 0) {
            builder.field(OPTIONS_FIELD.getPreferredName(), options);
        }
        if (requireFieldMatch != null) {
            builder.field(REQUIRE_FIELD_MATCH_FIELD.getPreferredName(), requireFieldMatch);
        }
        if (noMatchSize != null) {
            builder.field(NO_MATCH_SIZE_FIELD.getPreferredName(), noMatchSize);
        }
        if (phraseLimit != null) {
            builder.field(PHRASE_LIMIT_FIELD.getPreferredName(), phraseLimit);
        }
        if (maxAnalyzedOffset != null) {
            builder.field(MAX_ANALYZED_OFFSET_FIELD.getPreferredName(), maxAnalyzedOffset);
        }
    }

    static <HB extends AbstractHighlighterBuilder<HB>> BiFunction<XContentParser, HB, HB> setupParser(ObjectParser<HB, Void> parser) {
        parser.declareStringArray(fromList(String.class, HB::preTags), PRE_TAGS_FIELD);
        parser.declareStringArray(fromList(String.class, HB::postTags), POST_TAGS_FIELD);
        parser.declareString(HB::order, ORDER_FIELD);
        parser.declareBoolean(HB::highlightFilter, HIGHLIGHT_FILTER_FIELD);
        parser.declareInt(HB::fragmentSize, FRAGMENT_SIZE_FIELD);
        parser.declareInt(HB::numOfFragments, NUMBER_OF_FRAGMENTS_FIELD);
        parser.declareString(HB::encoder, ENCODER_FIELD);
        parser.declareString(HB::tagsSchema, TAGS_SCHEMA_FIELD);
        parser.declareBoolean(HB::requireFieldMatch, REQUIRE_FIELD_MATCH_FIELD);
        parser.declareString(HB::boundaryScannerType, BOUNDARY_SCANNER_FIELD);
        parser.declareInt(HB::boundaryMaxScan, BOUNDARY_MAX_SCAN_FIELD);
        parser.declareString((HB hb, String bc) -> hb.boundaryChars(bc.toCharArray()), BOUNDARY_CHARS_FIELD);
        parser.declareString(HB::boundaryScannerLocale, BOUNDARY_SCANNER_LOCALE_FIELD);
        parser.declareString(HB::highlighterType, TYPE_FIELD);
        parser.declareString(HB::fragmenter, FRAGMENTER_FIELD);
        parser.declareInt(HB::noMatchSize, NO_MATCH_SIZE_FIELD);
        parser.declareBoolean((builder, value) -> {}, FORCE_SOURCE_FIELD);  // force_source is ignored
        parser.declareInt(HB::phraseLimit, PHRASE_LIMIT_FIELD);
        parser.declareInt(HB::maxAnalyzedOffset, MAX_ANALYZED_OFFSET_FIELD);
        parser.declareObject(HB::options, (XContentParser p, Void c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new RuntimeException("Error parsing options", e);
            }
        }, OPTIONS_FIELD);
        parser.declareObject(HB::highlightQuery, (XContentParser p, Void c) -> {
            try {
                return parseTopLevelQuery(p);
            } catch (IOException e) {
                throw new RuntimeException("Error parsing query", e);
            }
        }, HIGHLIGHT_QUERY_FIELD);
        return (XContentParser p, HB hb) -> {
            try {
                parser.parse(p, hb, null);
                if (hb.preTags() != null && hb.postTags() == null) {
                    throw new ParsingException(p.getTokenLocation(), "pre_tags are set but post_tags are not set");
                }
                if (hb.preTags() != null && hb.postTags() != null && (hb.preTags().length == 0 || hb.postTags().length == 0)) {
                    throw new ParsingException(p.getTokenLocation(), "pre_tags or post_tags must not be empty");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return hb;
        };
    }

    @Override
    public final int hashCode() {
        return Objects.hash(
            getClass(),
            Arrays.hashCode(preTags),
            Arrays.hashCode(postTags),
            fragmentSize,
            numOfFragments,
            encoder,
            highlighterType,
            fragmenter,
            highlightQuery,
            order,
            highlightFilter,
            boundaryScannerType,
            boundaryMaxScan,
            Arrays.hashCode(boundaryChars),
            boundaryScannerLocale,
            noMatchSize,
            phraseLimit,
            options,
            requireFieldMatch,
            maxAnalyzedOffset,
            doHashCode()
        );
    }

    /**
     * fields only present in subclass should contribute to hashCode in the implementation
     */
    protected abstract int doHashCode();

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        HB other = (HB) obj;
        return Arrays.equals(preTags, other.preTags)
            && Arrays.equals(postTags, other.postTags)
            && Objects.equals(fragmentSize, other.fragmentSize)
            && Objects.equals(numOfFragments, other.numOfFragments)
            && Objects.equals(encoder, other.encoder)
            && Objects.equals(highlighterType, other.highlighterType)
            && Objects.equals(fragmenter, other.fragmenter)
            && Objects.equals(highlightQuery, other.highlightQuery)
            && Objects.equals(order, other.order)
            && Objects.equals(highlightFilter, other.highlightFilter)
            && Objects.equals(boundaryScannerType, other.boundaryScannerType)
            && Objects.equals(boundaryMaxScan, other.boundaryMaxScan)
            && Arrays.equals(boundaryChars, other.boundaryChars)
            && Objects.equals(boundaryScannerLocale, other.boundaryScannerLocale)
            && Objects.equals(noMatchSize, other.noMatchSize)
            && Objects.equals(phraseLimit, other.phraseLimit)
            && Objects.equals(options, other.options)
            && Objects.equals(requireFieldMatch, other.requireFieldMatch)
            && Objects.equals(maxAnalyzedOffset, other.maxAnalyzedOffset)
            && doEquals(other);
    }

    /**
     * fields only present in subclass should be checked for equality in the implementation
     */
    protected abstract boolean doEquals(HB other);

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
