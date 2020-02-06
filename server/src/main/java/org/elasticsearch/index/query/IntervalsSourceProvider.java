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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.XIntervals;
import org.apache.lucene.queries.intervals.FilteredIntervalsSource;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
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
import java.util.Set;

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

    public abstract void extractFields(Set<String> fields);

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
            case "prefix":
                return Prefix.fromXContent(parser);
            case "wildcard":
                return Wildcard.fromXContent(parser);
            case "fuzzy":
                return Fuzzy.fromXContent(parser);
        }
        throw new ParsingException(parser.getTokenLocation(),
            "Unknown interval type [" + parser.currentName() + "], expecting one of [match, any_of, all_of, prefix, wildcard]");
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
        private final String useField;

        public Match(String query, int maxGaps, boolean ordered, String analyzer, IntervalFilter filter, String useField) {
            this.query = query;
            this.maxGaps = maxGaps;
            this.ordered = ordered;
            this.analyzer = analyzer;
            this.filter = filter;
            this.useField = useField;
        }

        public Match(StreamInput in) throws IOException {
            this.query = in.readString();
            this.maxGaps = in.readVInt();
            this.ordered = in.readBoolean();
            this.analyzer = in.readOptionalString();
            this.filter = in.readOptionalWriteable(IntervalFilter::new);
            if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
                this.useField = in.readOptionalString();
            }
            else {
                this.useField = null;
            }
        }

        @Override
        public IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) throws IOException {
            NamedAnalyzer analyzer = null;
            if (this.analyzer != null) {
                analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            }
            IntervalsSource source;
            if (useField != null) {
                fieldType = context.fieldMapper(useField);
                assert fieldType != null;
                source = Intervals.fixField(useField, fieldType.intervals(query, maxGaps, ordered, analyzer, false));
            }
            else {
                source = fieldType.intervals(query, maxGaps, ordered, analyzer, false);
            }
            if (filter != null) {
                return filter.filter(source, context, fieldType);
            }
            return source;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
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
                Objects.equals(useField, match.useField) &&
                Objects.equals(analyzer, match.analyzer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, maxGaps, ordered, analyzer, filter, useField);
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
            if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
                out.writeOptionalString(useField);
            }
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
            if (useField != null) {
                builder.field("use_field", useField);
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
                String useField = (String) args[5];
                return new Match(query, max_gaps, ordered, analyzer, filter, useField);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("query"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_gaps"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Match fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        String getQuery() {
            return query;
        }

        int getMaxGaps() {
            return maxGaps;
        }

        boolean isOrdered() {
            return ordered;
        }

        String getAnalyzer() {
            return analyzer;
        }

        IntervalFilter getFilter() {
            return filter;
        }

        String getUseField() {
            return useField;
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
        public void extractFields(Set<String> fields) {
            for (IntervalsSourceProvider provider : subSources) {
                provider.extractFields(fields);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Disjunction that = (Disjunction) o;
            return Objects.equals(subSources, that.subSources) &&
                Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, filter);
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

        List<IntervalsSourceProvider> getSubSources() {
            return subSources;
        }

        IntervalFilter getFilter() {
            return filter;
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
        public void extractFields(Set<String> fields) {
            for (IntervalsSourceProvider provider : subSources) {
                provider.extractFields(fields);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Combine combine = (Combine) o;
            return Objects.equals(subSources, combine.subSources) &&
                ordered == combine.ordered &&
                maxGaps == combine.maxGaps &&
                Objects.equals(filter, combine.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, ordered, maxGaps, filter);
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

        List<IntervalsSourceProvider> getSubSources() {
            return subSources;
        }

        boolean isOrdered() {
            return ordered;
        }

        int getMaxGaps() {
            return maxGaps;
        }

        IntervalFilter getFilter() {
            return filter;
        }
    }

    public static class Prefix extends IntervalsSourceProvider {

        public static final String NAME = "prefix";

        private final String prefix;
        private final String analyzer;
        private final String useField;

        public Prefix(String prefix, String analyzer, String useField) {
            this.prefix = prefix;
            this.analyzer = analyzer;
            this.useField = useField;
        }

        public Prefix(StreamInput in) throws IOException {
            this.prefix = in.readString();
            this.analyzer = in.readOptionalString();
            this.useField = in.readOptionalString();
        }

        @Override
        public IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) throws IOException {
            NamedAnalyzer analyzer = null;
            if (this.analyzer != null) {
                analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            }
            IntervalsSource source;
            if (useField != null) {
                fieldType = context.fieldMapper(useField);
                assert fieldType != null;
                source = Intervals.fixField(useField, fieldType.intervals(prefix, 0, false, analyzer, true));
            }
            else {
                source = fieldType.intervals(prefix, 0, false, analyzer, true);
            }
            return source;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Prefix prefix = (Prefix) o;
            return Objects.equals(this.prefix, prefix.prefix) &&
                Objects.equals(analyzer, prefix.analyzer) &&
                Objects.equals(useField, prefix.useField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, analyzer, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(prefix);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("prefix", prefix);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Prefix, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            String analyzer = (String) args[1];
            String useField = (String) args[2];
            return new Prefix(term, analyzer, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("prefix"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Prefix fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        String getPrefix() {
            return prefix;
        }

        String getAnalyzer() {
            return analyzer;
        }

        String getUseField() {
            return useField;
        }
    }

    public static class Wildcard extends IntervalsSourceProvider {

        public static final String NAME = "wildcard";

        private final String pattern;
        private final String analyzer;
        private final String useField;

        public Wildcard(String pattern, String analyzer, String useField) {
            this.pattern = pattern;
            this.analyzer = analyzer;
            this.useField = useField;
        }

        public Wildcard(StreamInput in) throws IOException {
            this.pattern = in.readString();
            this.analyzer = in.readOptionalString();
            this.useField = in.readOptionalString();
        }

        @Override
        public IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) {
            NamedAnalyzer analyzer = fieldType.searchAnalyzer();
            if (this.analyzer != null) {
                analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            }
            IntervalsSource source;
            if (useField != null) {
                fieldType = context.fieldMapper(useField);
                assert fieldType != null;
                checkPositions(fieldType);
                if (this.analyzer == null) {
                    analyzer = fieldType.searchAnalyzer();
                }
                BytesRef normalizedTerm = analyzer.normalize(useField, pattern);
                // TODO Intervals.wildcard() should take BytesRef
                source = Intervals.fixField(useField, XIntervals.wildcard(normalizedTerm));
            }
            else {
                checkPositions(fieldType);
                BytesRef normalizedTerm = analyzer.normalize(fieldType.name(), pattern);
                source = XIntervals.wildcard(normalizedTerm);
            }
            return source;
        }

        private void checkPositions(MappedFieldType type) {
            if (type.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + type.name() + "] with no positions indexed");
            }
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Wildcard wildcard = (Wildcard) o;
            return Objects.equals(pattern, wildcard.pattern) &&
                Objects.equals(analyzer, wildcard.analyzer) &&
                Objects.equals(useField, wildcard.useField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, analyzer, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pattern);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("pattern", pattern);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Wildcard, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            String analyzer = (String) args[1];
            String useField = (String) args[2];
            return new Wildcard(term, analyzer, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("pattern"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Wildcard fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        String getPattern() {
            return pattern;
        }

        String getAnalyzer() {
            return analyzer;
        }

        String getUseField() {
            return useField;
        }
    }

    public static class Fuzzy extends IntervalsSourceProvider {

        public static final String NAME = "fuzzy";

        private final String term;
        private final int prefixLength;
        private final boolean transpositions;
        private final Fuzziness fuzziness;
        private final String analyzer;
        private final String useField;

        public Fuzzy(String term, int prefixLength, boolean transpositions, Fuzziness fuzziness, String analyzer, String useField) {
            this.term = term;
            this.prefixLength = prefixLength;
            this.transpositions = transpositions;
            this.fuzziness = fuzziness;
            this.analyzer = analyzer;
            this.useField = useField;
        }

        public Fuzzy(StreamInput in) throws IOException {
            this.term = in.readString();
            this.prefixLength = in.readVInt();
            this.transpositions = in.readBoolean();
            this.fuzziness = new Fuzziness(in);
            this.analyzer = in.readOptionalString();
            this.useField = in.readOptionalString();
        }

        @Override
        public IntervalsSource getSource(QueryShardContext context, MappedFieldType fieldType) {
            NamedAnalyzer analyzer = fieldType.searchAnalyzer();
            if (this.analyzer != null) {
                analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            }
            IntervalsSource source;
            if (useField != null) {
                fieldType = context.fieldMapper(useField);
                assert fieldType != null;
                checkPositions(fieldType);
                if (this.analyzer == null) {
                    analyzer = fieldType.searchAnalyzer();
                }
            }
            checkPositions(fieldType);
            BytesRef normalizedTerm = analyzer.normalize(fieldType.name(), term);
            FuzzyQuery fq = new FuzzyQuery(new Term(fieldType.name(), normalizedTerm),
                fuzziness.asDistance(term), prefixLength, 128, transpositions);
            CompiledAutomaton[] automata = fq.getAutomata();
            source = XIntervals.multiterm(automata[automata.length - 1], term);
            if (useField != null) {
                source = Intervals.fixField(useField, source);
            }
            return source;
        }

        private void checkPositions(MappedFieldType type) {
            if (type.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + type.name() + "] with no positions indexed");
            }
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fuzzy fuzzy = (Fuzzy) o;
            return prefixLength == fuzzy.prefixLength &&
                transpositions == fuzzy.transpositions &&
                Objects.equals(term, fuzzy.term) &&
                Objects.equals(fuzziness, fuzzy.fuzziness) &&
                Objects.equals(analyzer, fuzzy.analyzer) &&
                Objects.equals(useField, fuzzy.useField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, prefixLength, transpositions, fuzziness, analyzer, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeVInt(prefixLength);
            out.writeBoolean(transpositions);
            fuzziness.writeTo(out);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("term", term);
            builder.field("prefix_length", prefixLength);
            builder.field("transpositions", transpositions);
            fuzziness.toXContent(builder, params);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Fuzzy, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            int prefixLength = (args[1] == null) ? FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH : (int) args[1];
            boolean transpositions = (args[2] == null) ? FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS : (boolean) args[2];
            Fuzziness fuzziness = (args[3] == null) ? FuzzyQueryBuilder.DEFAULT_FUZZINESS : (Fuzziness) args[3];
            String analyzer = (String) args[4];
            String useField = (String) args[5];
            return new Fuzzy(term, prefixLength, transpositions, fuzziness, analyzer, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("term"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("prefix_length"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("transpositions"));
            PARSER.declareField(optionalConstructorArg(), (p, c) -> Fuzziness.parse(p), Fuzziness.FIELD, ObjectParser.ValueType.VALUE);
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Fuzzy fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        String getTerm() {
            return term;
        }

        int getPrefixLength() {
            return prefixLength;
        }

        boolean isTranspositions() {
            return transpositions;
        }

        Fuzziness getFuzziness() {
            return fuzziness;
        }

        String getAnalyzer() {
            return analyzer;
        }

        String getUseField() {
            return useField;
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

    public static class IntervalFilter implements ToXContentObject, Writeable {

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
                IntervalFilterScript ifs = context.compile(script, IntervalFilterScript.CONTEXT).newInstance();
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
                case "overlapping":
                    return Intervals.overlapping(input, filterSource);
                case "not_overlapping":
                    return Intervals.nonOverlapping(input, filterSource);
                case "before":
                    return Intervals.before(input, filterSource);
                case "after":
                    return Intervals.after(input, filterSource);
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
                Objects.equals(script, that.script) &&
                Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, filter, script);
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
            if (filter != null) {
                builder.startObject(type);
                filter.toXContent(builder, params);
                builder.endObject();
            } else {
                builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
            }
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

        String getType() {
            return type;
        }

        IntervalsSourceProvider getFilter() {
            return filter;
        }

        Script getScript() {
            return script;
        }
    }



}
