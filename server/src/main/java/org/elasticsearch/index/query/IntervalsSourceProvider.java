package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.Intervals;
import org.apache.lucene.search.intervals.IntervalsSource;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class IntervalsSourceProvider implements NamedWriteable, ToXContentObject {

    public abstract ESIntervalsSource getSource(MappedFieldType fieldType) throws IOException;

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    public static IntervalsSourceProvider fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Malformed IntervalsSource definition, expected start_object");
        }
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Malformed IntervalsSource definition, no field after start_object");
        }
        String sourceType = parser.currentName();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Malformed IntervalsSource definition, expected start_object after source name");
        }
        IntervalsSourceProvider provider = parser.namedObject(IntervalsSourceProvider.class, sourceType, null);
        //end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "[" + sourceType + "] malformed source, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        //end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "[" + sourceType + "] malformed source, expected [END_OBJECT] but found [" + parser.currentToken() + "]");
        }
        return provider;
    }

    public enum Type {

        ORDERED {
            @Override
            public ESIntervalsSource source(List<ESIntervalsSource> subSources) {
                return null;
            }
        },
        UNORDERED {
            @Override
            public ESIntervalsSource source(List<ESIntervalsSource> subSources) {
                return null;
            }
        },
        BLOCK {
            @Override
            public ESIntervalsSource source(List<ESIntervalsSource> subSources) {
                return PHRASE.source(subSources);
            }
        },
        PHRASE {
            @Override
            public ESIntervalsSource source(List<ESIntervalsSource> subSources) {
                return new ESIntervalsSource.PhraseIntervalsSource(subSources);
            }
        } ;

        public abstract ESIntervalsSource source(List<ESIntervalsSource> subSources);

    }

    public static class Match extends IntervalsSourceProvider {

        public static final String NAME = "match";

        private final String text;
        private final int maxWidth;
        private final Type type;

        public Match(String text, int maxWidth, Type type) {
            this.text = text;
            this.maxWidth = maxWidth;
            this.type = type;
        }

        public Match(StreamInput in) throws IOException {
            this.text = in.readString();
            this.maxWidth = in.readInt();
            this.type = in.readEnum(Type.class);
        }

        @Override
        public ESIntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot create source against field [" + fieldType.name() + "] with no positions indexed");
            }
            Analyzer analyzer = fieldType.searchAnalyzer();
            List<ESIntervalsSource> subSources = ESIntervalsSource.analyzeQuery(fieldType.name(), text, analyzer);
            ESIntervalsSource combination = type.source(subSources);
            if (maxWidth != Integer.MAX_VALUE) {
                return ESIntervalsSource.maxwidth(maxWidth, combination);
            }
            return combination;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Match match = (Match) o;
            return Objects.equals(text, match.text) && Objects.equals(maxWidth, match.maxWidth)
                && Objects.equals(type, match.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text, maxWidth, type);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(text);
            out.writeInt(maxWidth);
            out.writeEnum(type);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("text", text);
            builder.field("max_width", maxWidth);
            builder.field("type", type.toString());
            return builder.endObject().endObject();
        }

        private static final ConstructingObjectParser<Match, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                String text = (String) args[0];
                int max_width = (args[1] == null ? Integer.MAX_VALUE : (Integer) args[1]);
                Type type = (args[2] == null ? Type.ORDERED : (Type) args[2]);
                return new Match(text, max_width, type);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("text"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_width"));
            PARSER.declareField(optionalConstructorArg(), (p, c) -> Type.valueOf(p.text()),
                new ParseField("type"), ObjectParser.ValueType.STRING);
        }

        public static Match fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }
    }

    public static class Disjunction extends IntervalsSourceProvider {

        public static final String NAME = "or";

        private final List<IntervalsSourceProvider> subSources;

        public Disjunction(List<IntervalsSourceProvider> subSources) {
            this.subSources = subSources;
        }

        public Disjunction(StreamInput in) throws IOException {
            this.subSources = in.readNamedWriteableList(IntervalsSourceProvider.class);
        }

        @Override
        public ESIntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<ESIntervalsSource> sources = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                sources.add(provider.getSource(fieldType));
            }
            return ESIntervalsSource.disjunction(sources);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            return false;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(subSources);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.startArray("sources");
            for (IntervalsSourceProvider provider : subSources) {
                provider.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder.endObject();
        }

        public static Disjunction fromXContent(XContentParser parser) throws IOException {
            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw new ParsingException(parser.getTokenLocation(), "Expected start array");
            }
            List<IntervalsSourceProvider> subSources = new ArrayList<>();
            do {
                subSources.add(IntervalsSourceProvider.fromXContent(parser));
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY);
            return new Disjunction(subSources);
        }
    }

    public static class Combine extends IntervalsSourceProvider {

        public static final String NAME = "combine";

        private final List<IntervalsSourceProvider> subSources;
        private final Type type;
        private final int maxWidth;

        public Combine(List<IntervalsSourceProvider> subSources, Type type, int maxWidth) {
            this.subSources = subSources;
            this.type = type;
            this.maxWidth = maxWidth;
        }

        public Combine(StreamInput in) throws IOException {
            this.type = in.readEnum(Type.class);
            this.subSources = in.readNamedWriteableList(IntervalsSourceProvider.class);
            this.maxWidth = in.readInt();
        }

        @Override
        public ESIntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<ESIntervalsSource> ss = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                ss.add(provider.getSource(fieldType));
            }
            ESIntervalsSource source = type.source(ss);
            if (maxWidth == Integer.MAX_VALUE) {
                return source;
            }
            return ESIntervalsSource.maxwidth(maxWidth, source);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Combine combine = (Combine) o;
            return Objects.equals(subSources, combine.subSources) &&
                type == combine.type && maxWidth == combine.maxWidth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, type, maxWidth);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(type);
            out.writeNamedWriteableList(subSources);
            out.writeInt(maxWidth);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("type", type.toString().toLowerCase(Locale.ROOT));
            builder.field("max_width", maxWidth);
            builder.startArray("sources");
            for (IntervalsSourceProvider provider : subSources) {
                provider.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Combine, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                Type type = Type.valueOf(((String)args[0]).toUpperCase(Locale.ROOT));
                List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>)args[1];
                Integer maxWidth = (args[2] == null ? Integer.MAX_VALUE : (Integer)args[2]);
                return new Combine(subSources, type, maxWidth);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObjectArray(constructorArg(), (p, c) -> IntervalsSourceProvider.fromXContent(p), new ParseField("sources"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_width"));
        }

        public static Combine fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public static class Relate extends IntervalsSourceProvider {

        public static final String NAME = "relate";

        public enum Relation {
            CONTAINING {
                @Override
                ESIntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
                    return Intervals.containing(source, filter);
                }
            }, NOT_CONTAINING {
                @Override
                IntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
                    return Intervals.notContaining(source, filter);
                }
            }, CONTAINED_BY {
                @Override
                IntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
                    return Intervals.containedBy(source, filter);
                }
            }, NOT_CONTAINED_BY {
                @Override
                IntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
                    return Intervals.notContainedBy(source, filter);
                }
            }, NOT_OVERLAPPING {
                @Override
                IntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
                    return Intervals.nonOverlapping(source, filter);
                }
            };
            abstract ESIntervalsSource getSource(ESIntervalsSource source, ESIntervalsSource filter);
        }

        private final IntervalsSourceProvider source;
        private final IntervalsSourceProvider filter;
        private final Relation relation;

        public Relate(IntervalsSourceProvider source, IntervalsSourceProvider filter, Relation relation) {
            this.source = source;
            this.filter = filter;
            this.relation = relation;
        }

        public Relate(StreamInput in) throws IOException {
            this.source = in.readNamedWriteable(IntervalsSourceProvider.class);
            this.filter = in.readNamedWriteable(IntervalsSourceProvider.class);
            this.relation = in.readEnum(Relation.class);
        }

        @Override
        public ESIntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            IntervalsSource s = source.getSource(fieldType);
            IntervalsSource f = filter.getSource(fieldType);
            return relation.getSource(s, f);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Relate relate = (Relate) o;
            return Objects.equals(source, relate.source) &&
                Objects.equals(filter, relate.filter) &&
                relation == relate.relation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, filter, relation);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(source);
            out.writeNamedWriteable(filter);
            out.writeEnum(relation);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("source", source);
            builder.field("filter", filter);
            builder.field("relation", relation.toString().toLowerCase(Locale.ROOT));
            builder.endObject();
            builder.endObject();
            return builder;
        }

        static final ConstructingObjectParser<Relate, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                Relation relation = Relation.valueOf(((String)args[2]).toUpperCase(Locale.ROOT));
                return new Relate((IntervalsSourceProvider)args[0], (IntervalsSourceProvider)args[1], relation);
            });
        static {
            PARSER.declareObject(constructorArg(), (p, c) -> IntervalsSourceProvider.fromXContent(p), new ParseField("source"));
            PARSER.declareObject(constructorArg(), (p, c) -> IntervalsSourceProvider.fromXContent(p), new ParseField("filter"));
            PARSER.declareString(constructorArg(), new ParseField("relation"));
        }

        public static Relate fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

}
