package org.elasticsearch.index.query;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.Intervals;
import org.apache.lucene.search.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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

    public abstract IntervalsSource getSource(MappedFieldType fieldType) throws IOException;

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

    public static final IntervalsSource NO_INTERVALS = new IntervalsSource() {

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
            return new IntervalIterator() {
                @Override
                public int start() {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public int end() {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public int nextInterval() throws IOException {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public float matchCost() {
                    return 0;
                }

                @Override
                public int docID() {
                    return NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() throws IOException {
                    return NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) throws IOException {
                    return NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public void extractTerms(String field, Set<Term> terms) {

        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            return other == this;
        }

        @Override
        public String toString() {
            return "no_match";
        }
    };

    public static class Match extends IntervalsSourceProvider {

        public static final String NAME = "match";

        private final String text;
        private final int maxWidth;
        private final boolean ordered;

        public Match(String text, int maxWidth, boolean ordered) {
            this.text = text;
            this.maxWidth = maxWidth;
            this.ordered = ordered;
        }

        public Match(StreamInput in) throws IOException {
            this.text = in.readString();
            this.maxWidth = in.readInt();
            this.ordered = in.readBoolean();
        }

        @Override
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> subSources = new ArrayList<>();
            try (TokenStream ts = fieldType.tokenize(fieldType.name(), text)) {
                // TODO synonyms -> run through GraphTokenStreamFiniteStrings?
                TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
                ts.reset();
                while (ts.incrementToken()) {
                    BytesRef term = bytesAtt.getBytesRef();
                    subSources.add(Intervals.term(BytesRef.deepCopyOf(term)));
                }
                ts.end();
            }
            if (subSources.size() == 0) {
                return NO_INTERVALS;
            }
            if (subSources.size() == 1) {
                return subSources.get(0);
            }
            IntervalsSource source = ordered ?
                Intervals.ordered(subSources.toArray(new IntervalsSource[]{})) :
                Intervals.unordered(subSources.toArray(new IntervalsSource[]{}));
            if (maxWidth != Integer.MAX_VALUE) {
                return Intervals.maxwidth(maxWidth, source);
            }
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Match match = (Match) o;
            return Objects.equals(text, match.text) && Objects.equals(maxWidth, match.maxWidth)
                && Objects.equals(ordered, match.ordered);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text, maxWidth, ordered);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(text);
            out.writeInt(maxWidth);
            out.writeBoolean(ordered);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(NAME);
            builder.field("text", text);
            builder.field("max_width", maxWidth);
            builder.field("ordered", ordered);
            return builder.endObject().endObject();
        }

        private static final ConstructingObjectParser<Match, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                String text = (String) args[0];
                int max_width = (args[1] == null ? Integer.MAX_VALUE : (Integer) args[1]);
                boolean ordered = (args[2] == null ? false : (Boolean) args[2]);
                return new Match(text, max_width, ordered);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("text"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_width"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered"));
        }

        public static Match fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }
    }

    public static class Combine extends IntervalsSourceProvider {

        public static final String NAME = "combine";

        protected enum Type {
            ORDERED {
                @Override
                IntervalsSource getSource(List<IntervalsSource> subSources) {
                    return Intervals.ordered(subSources.toArray(new IntervalsSource[0]));
                }
            }, UNORDERED {
                @Override
                IntervalsSource getSource(List<IntervalsSource> subSources) {
                    return Intervals.unordered(subSources.toArray(new IntervalsSource[0]));
                }
            }, OR {
                @Override
                IntervalsSource getSource(List<IntervalsSource> subSources) {
                    return Intervals.or(subSources.toArray(new IntervalsSource[0]));
                }
            };

            abstract IntervalsSource getSource(List<IntervalsSource> subSources);
        }

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
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> ss = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                ss.add(provider.getSource(fieldType));
            }
            IntervalsSource source = type.getSource(ss);
            if (maxWidth == Integer.MAX_VALUE) {
                return source;
            }
            return Intervals.maxwidth(maxWidth, source);
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

}
