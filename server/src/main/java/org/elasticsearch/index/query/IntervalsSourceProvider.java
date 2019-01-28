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

package org.elasticsearch.index.query;

import org.apache.lucene.search.intervals.FilteredIntervalsSource;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.Intervals;
import org.apache.lucene.search.intervals.IntervalsSource;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Factory class for {@link IntervalsSource}
 *
 * Built-in sources include {@link Match}, which analyzes a text string and converts it
 * to a proximity source (phrase, ordered or unordered depending on how
 * strict the matching should be); {@link Combine}, which allows proximity queries
 * between different sub-sources; and {@link Disjunction}.
 */
public abstract class IntervalsSourceProvider implements NamedWriteable, ToXContentFragment {

    public abstract IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) throws IOException;

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    public static IntervalsSourceProvider fromXContent(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.FIELD_NAME;
        switch (parser.currentName()) {
            case "match":
                return Match.fromXContent(parser);
            case "any_of":
                return Disjunction.fromXContent(parser);
            case "all_of":
                return Combine.fromXContent(parser);
        }
        throw new ParsingException(parser.getTokenLocation(),
            "Unknown interval type [" + parser.currentName() + "], expecting one of [match, any_of, all_of]");
    }

    private static IntervalsSourceProvider parseInnerIntervals(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
        }
        IntervalsSourceProvider isp = IntervalsSourceProvider.fromXContent(parser);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }
        return isp;
    }

    public static class Match extends IntervalsSourceProvider {

        public static final String NAME = "match";

        private final String query;
        private final int maxGaps;
        private final boolean ordered;
        private final String analyzer;
        private final IntervalFilter filter;

        public Match(String query, int maxGaps, boolean ordered, String analyzer, IntervalFilter filter) {
            this.query = query;
            this.maxGaps = maxGaps;
            this.ordered = ordered;
            this.analyzer = analyzer;
            this.filter = filter;
        }

        public Match(StreamInput in) throws IOException {
            this.query = in.readString();
            this.maxGaps = in.readVInt();
            this.ordered = in.readBoolean();
            this.analyzer = in.readOptionalString();
            this.filter = in.readOptionalWriteable(IntervalFilter::new);
        }

        @Override
        public IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) throws IOException {
            NamedAnalyzer analyzer = null;
            if (this.analyzer != null) {
                analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            }
            IntervalsSource source = fieldType.intervals(query, maxGaps, ordered, analyzer);
            if (filter != null) {
                return filter.filter(source, context, fieldType);
            }
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Match match = (Match) o;
            return maxGaps == match.maxGaps &&
                ordered == match.ordered &&
                Objects.equals(query, match.query) &&
                Objects.equals(filter, match.filter) &&
                Objects.equals(analyzer, match.analyzer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, maxGaps, ordered, analyzer, filter);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(query);
            out.writeVInt(maxGaps);
            out.writeBoolean(ordered);
            out.writeOptionalString(analyzer);
            out.writeOptionalWriteable(filter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            builder.startObject();
            builder.field("query", query);
            builder.field("max_gaps", maxGaps);
            builder.field("ordered", ordered);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (filter != null) {
                builder.field("filter", filter);
            }
            return builder.endObject();
        }

        private static final ConstructingObjectParser<Match, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                String query = (String) args[0];
                int max_gaps = (args[1] == null ? -1 : (Integer) args[1]);
                boolean ordered = (args[2] != null && (boolean) args[2]);
                String analyzer = (String) args[3];
                IntervalFilter filter = (IntervalFilter) args[4];
                return new Match(query, max_gaps, ordered, analyzer, filter);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("query"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_gaps"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
        }

        public static Match fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public static class Disjunction extends IntervalsSourceProvider {

        public static final String NAME = "any_of";

        private final List<IntervalsSourceProvider> subSources;
        private final IntervalFilter filter;

        public Disjunction(List<IntervalsSourceProvider> subSources, IntervalFilter filter) {
            this.subSources = subSources;
            this.filter = filter;
        }

        public Disjunction(StreamInput in) throws IOException {
            this.subSources = in.readNamedWriteableList(IntervalsSourceProvider.class);
            this.filter = in.readOptionalWriteable(IntervalFilter::new);
        }

        @Override
        public IntervalsSource getSource(QueryShardContext ctx, MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> sources = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                sources.add(provider.getSource(ctx, fieldType));
            }
            IntervalsSource source = Intervals.or(sources.toArray(new IntervalsSource[0]));
            if (filter == null) {
                return source;
            }
            return filter.filter(source, ctx, fieldType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Disjunction that = (Disjunction) o;
            return Objects.equals(subSources, that.subSources);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(subSources);
            out.writeOptionalWriteable(filter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.startArray("intervals");
            for (IntervalsSourceProvider provider : subSources) {
                builder.startObject();
                provider.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            if (filter != null) {
                builder.field("filter", filter);
            }
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Disjunction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>)args[0];
                IntervalFilter filter = (IntervalFilter) args[1];
                return new Disjunction(subSources, filter);
            });
        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> IntervalsSourceProvider.parseInnerIntervals(p),
                new ParseField("intervals"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p),
                new ParseField("filter"));
        }

        public static Disjunction fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    public static class Combine extends IntervalsSourceProvider {

        public static final String NAME = "all_of";

        private final List<IntervalsSourceProvider> subSources;
        private final boolean ordered;
        private final int maxGaps;
        private final IntervalFilter filter;

        public Combine(List<IntervalsSourceProvider> subSources, boolean ordered, int maxGaps, IntervalFilter filter) {
            this.subSources = subSources;
            this.ordered = ordered;
            this.maxGaps = maxGaps;
            this.filter = filter;
        }

        public Combine(StreamInput in) throws IOException {
            this.ordered = in.readBoolean();
            this.subSources = in.readNamedWriteableList(IntervalsSourceProvider.class);
            this.maxGaps = in.readInt();
            this.filter = in.readOptionalWriteable(IntervalFilter::new);
        }

        @Override
        public IntervalsSource getSource(QueryShardContext ctx, MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> ss = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                ss.add(provider.getSource(ctx, fieldType));
            }
            IntervalsSource source = IntervalBuilder.combineSources(ss, maxGaps, ordered);
            if (filter != null) {
                return filter.filter(source, ctx, fieldType);
            }
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Combine combine = (Combine) o;
            return Objects.equals(subSources, combine.subSources) &&
                ordered == combine.ordered && maxGaps == combine.maxGaps;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, ordered, maxGaps);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(ordered);
            out.writeNamedWriteableList(subSources);
            out.writeInt(maxGaps);
            out.writeOptionalWriteable(filter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("ordered", ordered);
            builder.field("max_gaps", maxGaps);
            builder.startArray("intervals");
            for (IntervalsSourceProvider provider : subSources) {
                builder.startObject();
                provider.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            if (filter != null) {
                builder.field("filter", filter);
            }
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Combine, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                boolean ordered = (args[0] != null && (boolean) args[0]);
                List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>)args[1];
                Integer maxGaps = (args[2] == null ? -1 : (Integer)args[2]);
                IntervalFilter filter = (IntervalFilter) args[3];
                return new Combine(subSources, ordered, maxGaps, filter);
            });
        static {
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered"));
            PARSER.declareObjectArray(constructorArg(), (p, c) -> IntervalsSourceProvider.parseInnerIntervals(p),
                new ParseField("intervals"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_gaps"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
        }

        public static Combine fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    static class ScriptFilterSource extends FilteredIntervalsSource {

        final IntervalFilterScript script;
        IntervalFilterScript.Interval interval = new IntervalFilterScript.Interval();

        ScriptFilterSource(IntervalsSource in, String name, IntervalFilterScript script) {
            super("FILTER(" + name + ")", in);
            this.script = script;
        }

        @Override
        protected boolean accept(IntervalIterator it) {
            interval.setIterator(it);
            return script.execute(interval);
        }
    }

    public static class IntervalFilter implements ToXContent, Writeable {

        public static final String NAME = "filter";

        private final String type;
        private final IntervalsSourceProvider filter;
        private final Script script;

        public IntervalFilter(IntervalsSourceProvider filter, String type) {
            this.filter = filter;
            this.type = type.toLowerCase(Locale.ROOT);
            this.script = null;
        }

        IntervalFilter(Script script) {
            this.script = script;
            this.type = "script";
            this.filter = null;
        }

        public IntervalFilter(StreamInput in) throws IOException {
            this.type = in.readString();
            this.filter = in.readOptionalNamedWriteable(IntervalsSourceProvider.class);
            if (in.readBoolean()) {
                this.script = new Script(in);
            }
            else {
                this.script = null;
            }
        }

        public IntervalsSource filter(IntervalsSource input, QueryShardContext context, MappedFieldType fieldType) throws IOException {
            if (script != null) {
                IntervalFilterScript ifs = context.getScriptService().compile(script, IntervalFilterScript.CONTEXT).newInstance();
                return new ScriptFilterSource(input, script.getIdOrCode(), ifs);
            }
            IntervalsSource filterSource = filter.getSource(context, fieldType);
            switch (type) {
                case "containing":
                    return Intervals.containing(input, filterSource);
                case "contained_by":
                    return Intervals.containedBy(input, filterSource);
                case "not_containing":
                    return Intervals.notContaining(input, filterSource);
                case "not_contained_by":
                    return Intervals.notContainedBy(input, filterSource);
                case "not_overlapping":
                    return Intervals.nonOverlapping(input, filterSource);
                default:
                    throw new IllegalArgumentException("Unknown filter type [" + type + "]");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntervalFilter that = (IntervalFilter) o;
            return Objects.equals(type, that.type) &&
                Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, filter);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            out.writeOptionalNamedWriteable(filter);
            if (script == null) {
                out.writeBoolean(false);
            }
            else {
                out.writeBoolean(true);
                script.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(type);
            builder.startObject();
            filter.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        public static IntervalFilter fromXContent(XContentParser parser) throws IOException {
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            String type = parser.currentName();
            if (Script.SCRIPT_PARSE_FIELD.match(type, parser.getDeprecationHandler())) {
                Script script = Script.parse(parser);
                if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
                }
                return new IntervalFilter(script);
            }
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [START_OBJECT] but got [" + parser.currentToken() + "]");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            IntervalsSourceProvider intervals = IntervalsSourceProvider.fromXContent(parser);
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
            }
            return new IntervalFilter(intervals, type);
        }
    }



}
