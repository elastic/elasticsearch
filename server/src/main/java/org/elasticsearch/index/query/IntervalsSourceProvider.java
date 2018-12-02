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
            throw new ParsingException(parser.getTokenLocation(),
                "Malformed IntervalsSource definition, expected [start_object] but found [" + parser.currentToken() + "]");
        }
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(),
                "Malformed IntervalsSource definition, no field after start_object");
        }
        String sourceType = parser.currentName();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "Malformed IntervalsSource definition, expected [start_object] after source name but found ["
                    + parser.currentToken() + "]");
        }
        IntervalsSourceProvider provider = parser.namedObject(IntervalsSourceProvider.class, sourceType, null);
        //end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "[" + sourceType + "] malformed source, expected [end_object] but found [" + parser.currentToken() + "]");
        }
        //end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "[" + sourceType + "] malformed source, expected [end_object] but found [" + parser.currentToken() + "]");
        }
        return provider;
    }

    public enum Type {

        ORDERED {
            @Override
            public IntervalsSource source(List<IntervalsSource> subSources) {
                if (subSources.size() == 1) {
                    return subSources.get(0);
                }
                return Intervals.ordered(subSources.toArray(new IntervalsSource[0]));
            }
        },
        UNORDERED {
            @Override
            public IntervalsSource source(List<IntervalsSource> subSources) {
                if (subSources.size() == 1) {
                    return subSources.get(0);
                }
                return Intervals.unordered(subSources.toArray(new IntervalsSource[0]));
            }
        },
        BLOCK {
            @Override
            public IntervalsSource source(List<IntervalsSource> subSources) {
                if (subSources.size() == 1) {
                    return subSources.get(0);
                }
                return Intervals.phrase(subSources.toArray(new IntervalsSource[0]));
            }
        },
        PHRASE {
            @Override
            public IntervalsSource source(List<IntervalsSource> subSources) {
                return BLOCK.source(subSources);
            }
        } ;

        public abstract IntervalsSource source(List<IntervalsSource> subSources);

    }

    public static class Match extends IntervalsSourceProvider {

        public static final String NAME = "match";

        private final String text;
        private final int maxWidth;
        private final MappedFieldType.IntervalType type;

        public Match(String text, int maxWidth, MappedFieldType.IntervalType type) {
            this.text = text;
            this.maxWidth = maxWidth;
            this.type = type;
        }

        public Match(StreamInput in) throws IOException {
            this.text = in.readString();
            this.maxWidth = in.readInt();
            this.type = in.readEnum(MappedFieldType.IntervalType.class);
        }

        @Override
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            IntervalsSource source = fieldType.intervals(text, type);
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
            builder.field("type", type.toString().toLowerCase(Locale.ROOT));
            return builder.endObject().endObject();
        }

        private static final ConstructingObjectParser<Match, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                String text = (String) args[0];
                int max_width = (args[1] == null ? Integer.MAX_VALUE : (Integer) args[1]);
                MappedFieldType.IntervalType type = (args[2] == null
                    ? MappedFieldType.IntervalType.UNORDERED
                    : (MappedFieldType.IntervalType) args[2]);
                return new Match(text, max_width, type);
            });
        static {
            PARSER.declareString(constructorArg(), new ParseField("text"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_width"));
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> MappedFieldType.IntervalType.valueOf(p.text().toUpperCase(Locale.ROOT)),
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
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> sources = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                sources.add(provider.getSource(fieldType));
            }
            return Intervals.or(sources.toArray(new IntervalsSource[0]));
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

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Disjunction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>)args[0];
                return new Disjunction(subSources);
            });
        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> IntervalsSourceProvider.fromXContent(p), new ParseField("sources"));
        }

        public static Disjunction fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
            List<IntervalsSource> ss = new ArrayList<>();
            for (IntervalsSourceProvider provider : subSources) {
                ss.add(provider.getSource(fieldType));
            }
            IntervalsSource source = type.source(ss);
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

    public static class Relate extends IntervalsSourceProvider {

        public static final String NAME = "relate";

        public enum Relation {
            CONTAINING {
                @Override
                IntervalsSource getSource(IntervalsSource source, IntervalsSource filter) {
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
            abstract IntervalsSource getSource(IntervalsSource source, IntervalsSource filter);
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
        public IntervalsSource getSource(MappedFieldType fieldType) throws IOException {
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
