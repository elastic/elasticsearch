/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The breakdown of codec reads that a profiled request asks for. The profile always reports the codec totals. The
 * selection changes only what is reported. It never changes what is counted, the codec totals, the query results or how
 * the query runs. {@link CodecReadAttribution} describes how a selection maps readers to details.
 *
 * <p>The selection is the object form of {@code profile}. It has two independent dimensions:
 * <ul>
 *   <li>{@code true} or {@code {}}: codec totals only.</li>
 *   <li>{@code {"segments": ["_0", "_1"]}}: the totals of those segments over all fields.</li>
 *   <li>{@code {"fields": ["agent.id"]}}: the totals of those fields over all segments.</li>
 *   <li>{@code {"segments": ["_0"], "fields": ["agent.id"]}}: both, and also each selected field within each selected
 *   segment. A segment total still covers all fields, and a field total still covers all segments.</li>
 *   <li>{@code {"segments": "_all"}}: the totals of all segments, with no field breakdown.</li>
 *   <li>{@code {"fields": "_all"}}: the totals of all fields, with no segment breakdown.</li>
 *   <li>{@code {"segments": "_all", "fields": "_all"}}: all segments, all fields, and each field within each
 *   segment.</li>
 *   <li>{@code {"segments": "_all", "count_nested_docs": true}}: also counts the root and nested documents of each
 *   segment that has document blocks. This reads the parent field of the segment once.</li>
 * </ul>
 *
 * <p>A dimension expands only when the request asks for it: segments never expand fields, and fields never expand
 * segments. {@value #ALL} on a dimension also adds the descriptions of that dimension: the segment description for
 * segments, and the {@code field_info} and codec {@code column} facts for fields. An array entry is always a name, never
 * {@value #ALL}. Names match by exact string equality, without patterns. A segment name is either its Lucene name,
 * which matches in every shard, or its segment key {@code <index>:<shard>/<segment>}, which matches in one shard. A name
 * that matches nothing selects nothing, and an empty array selects nothing.
 */
public final class ProfileSelection implements Writeable {

    /**
     * Controls the availability of codec read profiling: the object form of {@code profile} and the codec reads of a
     * profiled request. When it is disabled, requests are parsed and profiled without codec reads.
     */
    public static final FeatureFlag CODEC_READ_PROFILING_FEATURE_FLAG = new FeatureFlag("codec_read_profiling");

    /** The reserved value that selects all segments or all fields. */
    public static final String ALL = "_all";

    public static final ParseField SEGMENTS_FIELD = new ParseField("segments");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField COUNT_NESTED_DOCS_FIELD = new ParseField("count_nested_docs");

    /** Codec totals only. */
    public static final ProfileSelection NONE = new ProfileSelection(null, null, false);

    /** All segments, all fields, and each field within each segment, with their descriptions. */
    public static final ProfileSelection EVERYTHING = new ProfileSelection(Names.EVERY, Names.EVERY, false);

    /**
     * Returns {@code requested}, or null when the feature flag disables codec read profiling on this node. A null
     * selection means that codec reads are not profiled. {@code SearchService} and the ES|QL {@code ComputeService} call
     * this for a profiled request; no other code decides availability.
     */
    @Nullable
    public static ProfileSelection ifEnabled(ProfileSelection requested) {
        return CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled() ? requested : null;
    }

    /** Null when no segment is selected. */
    @Nullable
    private final Names segments;
    /** Null when no field is selected. */
    @Nullable
    private final Names fields;
    /** Whether a described segment with document blocks counts its root and nested documents. */
    private final boolean countNestedDocs;

    private ProfileSelection(@Nullable Names segments, @Nullable Names fields, boolean countNestedDocs) {
        assert segments == null || segments.isNone() == false : "no segment is null";
        assert fields == null || fields.isNone() == false : "no field is null";
        this.segments = segments;
        this.fields = fields;
        this.countNestedDocs = countNestedDocs;
    }

    /** Selects these segments and fields. Null or no names selects nothing in that dimension. */
    public static ProfileSelection of(@Nullable Names segments, @Nullable Names fields) {
        final Names selectedSegments = segments == null || segments.isNone() ? null : segments;
        final Names selectedFields = fields == null || fields.isNone() ? null : fields;
        if (selectedSegments == null && selectedFields == null) {
            return NONE;
        }
        return new ProfileSelection(selectedSegments, selectedFields, false);
    }

    /**
     * Returns this selection with {@code count_nested_docs} set. It has an effect only with {@code "segments": "_all"}.
     * Counting reads the parent field once for each segment with document blocks. A segment without blocks has only
     * root documents and needs no read.
     */
    public ProfileSelection withCountNestedDocs(boolean countNestedDocs) {
        return countNestedDocs == this.countNestedDocs ? this : new ProfileSelection(segments, fields, countNestedDocs);
    }

    /** Whether {@code count_nested_docs} is set. */
    public boolean countNestedDocs() {
        return countNestedDocs;
    }

    public static ProfileSelection readFrom(StreamInput in) throws IOException {
        final Names segments = in.readOptionalWriteable(Names::readFrom);
        final Names fields = in.readOptionalWriteable(Names::readFrom);
        return of(segments, fields).withCountNestedDocs(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(segments);
        out.writeOptionalWriteable(fields);
        out.writeBoolean(countNestedDocs);
    }

    /**
     * Whether a segment is selected.
     *
     * @param segment   the Lucene name of the segment
     * @param qualified the segment key, or null when the shard is not known
     */
    public boolean selectsSegment(String segment, @Nullable String qualified) {
        return segments != null && (segments.includes(segment) || (qualified != null && segments.includes(qualified)));
    }

    /** Whether a field is selected. */
    public boolean selectsField(String field) {
        return fields != null && fields.includes(field);
    }

    /** Whether a selected segment reports {@code field} on its own. This is true only for a selected field. */
    public boolean breaksDown(String field) {
        return fields != null && fields.includes(field);
    }

    /** Whether the {@code fields} view is reported. */
    public boolean reportsFields() {
        return fields != null;
    }

    /** Whether segment descriptions are reported, which {@code "segments": "_all"} requests. */
    public boolean describesSegments() {
        return segments != null && segments.every;
    }

    /** Whether field descriptions and column facts are reported, which {@code "fields": "_all"} requests. */
    public boolean describesFields() {
        return fields != null && fields.every;
    }

    /** Whether only the codec totals are reported. */
    public boolean isNone() {
        return segments == null && fields == null;
    }

    /**
     * Parses the object form of {@code profile}. The parser starts on {@link XContentParser.Token#START_OBJECT} and ends on
     * the matching {@link XContentParser.Token#END_OBJECT}.
     */
    public static ProfileSelection parseProfileObject(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(parser.getTokenLocation(), "[profile] must be a boolean or an object");
        }
        Names segments = null;
        Names fields = null;
        boolean countNestedDocs = false;
        String name = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                name = parser.currentName();
            } else if (SEGMENTS_FIELD.match(name, parser.getDeprecationHandler())) {
                segments = Names.parse(parser, "segment");
            } else if (FIELDS_FIELD.match(name, parser.getDeprecationHandler())) {
                fields = Names.parse(parser, "field");
            } else if (COUNT_NESTED_DOCS_FIELD.match(name, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.VALUE_BOOLEAN) {
                    throw new XContentParseException(parser.getTokenLocation(), "[profile.count_nested_docs] must be a boolean");
                }
                countNestedDocs = parser.booleanValue();
            } else {
                throw new XContentParseException(parser.getTokenLocation(), "[profile] unknown field [" + name + "]");
            }
        }
        return of(segments, fields).withCountNestedDocs(countNestedDocs);
    }

    /** Writes the object form of {@code profile} that {@link #parseProfileObject} reads back. */
    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        if (segments != null) {
            segments.toXContent(builder, SEGMENTS_FIELD.getPreferredName());
        }
        if (fields != null) {
            fields.toXContent(builder, FIELDS_FIELD.getPreferredName());
        }
        if (countNestedDocs) {
            builder.field(COUNT_NESTED_DOCS_FIELD.getPreferredName(), true);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProfileSelection other
            && Objects.equals(segments, other.segments)
            && Objects.equals(fields, other.fields)
            && countNestedDocs == other.countNestedDocs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segments, fields, countNestedDocs);
    }

    @Override
    public String toString() {
        return "segments=" + segments + ", fields=" + fields + (countNestedDocs ? ", counting nested docs" : "");
    }

    /** The names that one dimension selects: all names, or exactly these. */
    public static final class Names implements Writeable {

        /** All names. */
        public static final Names EVERY = new Names(true, Set.of());

        private final boolean every;
        private final Set<String> names;

        private Names(boolean every, Set<String> names) {
            this.every = every;
            this.names = names;
        }

        /** Exactly these names. */
        public static Names of(Collection<String> names) {
            return new Names(false, Collections.unmodifiableSet(new TreeSet<>(names)));
        }

        static Names readFrom(StreamInput in) throws IOException {
            final boolean every = in.readBoolean();
            final Set<String> names = in.readCollectionAsSet(StreamInput::readString);
            return every ? EVERY : of(names);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(every);
            out.writeStringCollection(names);
        }

        boolean includes(String name) {
            return every || names.contains(name);
        }

        boolean isNone() {
            return every == false && names.isEmpty();
        }

        static Names parse(XContentParser parser, String kind) throws IOException {
            final XContentParser.Token token = parser.currentToken();
            final String option = "[profile." + kind + "s]";
            if (token == XContentParser.Token.VALUE_STRING) {
                if (ALL.equals(parser.text())) {
                    return EVERY;
                }
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    option + " must be [" + ALL + "] or an array of " + kind + " names but was [" + parser.text() + "]"
                );
            }
            if (token != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    option + " must be [" + ALL + "] or an array of " + kind + " names"
                );
            }
            final Set<String> names = new TreeSet<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.VALUE_STRING) {
                    throw new XContentParseException(parser.getTokenLocation(), option + " must hold " + kind + " names only");
                }
                names.add(parser.text());
            }
            return of(names);
        }

        void toXContent(XContentBuilder builder, String option) throws IOException {
            if (every) {
                builder.field(option, ALL);
            } else {
                builder.stringListField(option, names);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Names other && every == other.every && names.equals(other.names);
        }

        @Override
        public int hashCode() {
            return Boolean.hashCode(every) * 31 + names.hashCode();
        }

        @Override
        public String toString() {
            return every ? ALL : names.toString();
        }
    }
}
