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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public abstract class SortBuilder<T extends SortBuilder<T>> implements NamedWriteable, ToXContentObject, Rewriteable<SortBuilder<?>> {

    protected SortOrder order = SortOrder.ASC;

    // parse fields common to more than one SortBuilder
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField NESTED_FILTER_FIELD = new ParseField("nested_filter");
    public static final ParseField NESTED_PATH_FIELD = new ParseField("nested_path");
    public static final ParseField NESTED_FIELD = new ParseField("nested");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    private static final Map<String, Parser<?>> PARSERS;
    static {
        Map<String, Parser<?>> parsers = new HashMap<>();
        parsers.put(ScriptSortBuilder.NAME, ScriptSortBuilder::fromXContent);
        parsers.put(GeoDistanceSortBuilder.NAME, GeoDistanceSortBuilder::fromXContent);
        parsers.put(GeoDistanceSortBuilder.ALTERNATIVE_NAME, GeoDistanceSortBuilder::fromXContent);
        parsers.put(ScoreSortBuilder.NAME, ScoreSortBuilder::fromXContent);
        // FieldSortBuilder gets involved if the user specifies a name that isn't one of these.
        PARSERS = unmodifiableMap(parsers);
    }

    public static class NestedSort implements Writeable, Reader<NestedSort>, ToXContentObject {
        private String path;
        private QueryBuilder filter;
        private NestedSort nestedSort;

        public NestedSort() {
        }

        public NestedSort(StreamInput in) throws IOException {
            read(in);
        }

        public String getPath() {
            return path;
        }

        public NestedSort setPath(final String path) {
            this.path = path;
            return this;
        }

        public QueryBuilder getFilter() {
            return filter;
        }

        public NestedSort setFilter(final QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public NestedSort getNestedSort() {
            return nestedSort;
        }

        public NestedSort setNestedSort(final NestedSort nestedSort) {
            this.nestedSort = nestedSort;
            return this;
        }

        /**
         * Write this object's fields to a {@linkplain StreamOutput}.
         */
        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeOptionalString(path);
            out.writeOptionalNamedWriteable(filter);
            out.writeOptionalWriteable(nestedSort);
        }

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        @Override
        public NestedSort read(final StreamInput in) throws IOException {
            path = in.readOptionalString();
            filter = in.readOptionalNamedWriteable(QueryBuilder.class);
            nestedSort = in.readOptionalWriteable(NestedSort::new);
            return this;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            if (path != null) {
                builder.field(PATH_FIELD.getPreferredName(), path);
            }
            if (filter != null) {
                builder.field(FILTER_FIELD.getPreferredName(), filter);
            }
            if (nestedSort != null) {
                builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
            }
            builder.endObject();
            return builder;
        }

        public static NestedSort fromXContent(XContentParser parser) throws IOException {
            NestedSort nestedSort = new NestedSort();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentName = parser.currentName();
                        parser.nextToken();
                        if (currentName.equals(PATH_FIELD.getPreferredName())) {
                            nestedSort.setPath(parser.text());
                        } else if (currentName.equals(FILTER_FIELD.getPreferredName())) {
                            nestedSort.setFilter(parseNestedFilter(parser));
                        } else if (currentName.equals(NESTED_FIELD.getPreferredName())) {
                            nestedSort.setNestedSort(NestedSort.fromXContent(parser));
                        } else {
                            throw new IllegalArgumentException("malformed nested sort format, unknown field name [" + currentName + "]");
                        }
                    } else {
                        throw new IllegalArgumentException("malformed nested sort format, only field names are allowed");
                    }
                }
            } else {
                throw new IllegalArgumentException("malformed nested sort format, must start with an object");
            }

            return nestedSort;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NestedSort that = (NestedSort) obj;
            return Objects.equals(path, that.path)
                && Objects.equals(filter, that.filter)
                && Objects.equals(nestedSort, that.nestedSort);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, filter, nestedSort);
        }
    }

    /**
     * Create a @link {@link SortFieldAndFormat} from this builder.
     */
    protected abstract SortFieldAndFormat build(QueryShardContext context) throws IOException;

    /**
     * Set the order of sorting.
     */
    @SuppressWarnings("unchecked")
    public T order(SortOrder order) {
        Objects.requireNonNull(order, "sort order cannot be null.");
        this.order = order;
        return (T) this;
    }

    /**
     * Return the {@link SortOrder} used for this {@link SortBuilder}.
     */
    public SortOrder order() {
        return this.order;
    }

    public static List<SortBuilder<?>> fromXContent(XContentParser parser) throws IOException {
        List<SortBuilder<?>> sortFields = new ArrayList<>(2);
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    parseCompoundSortField(parser, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    String fieldName = parser.text();
                    sortFields.add(fieldOrScoreSort(fieldName));
                } else {
                    throw new IllegalArgumentException("malformed sort format, "
                            + "within the sort array, an object, or an actual string are allowed");
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            String fieldName = parser.text();
            sortFields.add(fieldOrScoreSort(fieldName));
        } else if (token == XContentParser.Token.START_OBJECT) {
            parseCompoundSortField(parser, sortFields);
        } else {
            throw new IllegalArgumentException("malformed sort format, either start with array, object, or an actual string");
        }
        return sortFields;
    }

    private static SortBuilder<?> fieldOrScoreSort(String fieldName) {
        if (fieldName.equals(ScoreSortBuilder.NAME)) {
            return new ScoreSortBuilder();
        } else {
            return new FieldSortBuilder(fieldName);
        }
    }

    private static void parseCompoundSortField(XContentParser parser, List<SortBuilder<?>> sortFields)
            throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    SortOrder order = SortOrder.fromString(parser.text());
                    sortFields.add(fieldOrScoreSort(fieldName).order(order));
                } else {
                    if (PARSERS.containsKey(fieldName)) {
                        sortFields.add(PARSERS.get(fieldName).fromXContent(parser, fieldName));
                    } else {
                        sortFields.add(FieldSortBuilder.fromXContent(parser, fieldName));
                    }
                }
            }
        }
    }

    public static Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders, QueryShardContext context) throws IOException {
        List<SortField> sortFields = new ArrayList<>(sortBuilders.size());
        List<DocValueFormat> sortFormats = new ArrayList<>(sortBuilders.size());
        for (SortBuilder<?> builder : sortBuilders) {
            SortFieldAndFormat sf = builder.build(context);
            sortFields.add(sf.field);
            sortFormats.add(sf.format);
        }
        if (!sortFields.isEmpty()) {
            // optimize if we just sort on score non reversed, we don't really
            // need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && !sortField.getReverse()) {
                    sort = false;
                } else {
                    sort = true;
                }
            }
            if (sort) {
                return Optional.of(new SortAndFormats(
                        new Sort(sortFields.toArray(new SortField[sortFields.size()])),
                        sortFormats.toArray(new DocValueFormat[sortFormats.size()])));
            }
        }
        return Optional.empty();
    }

    protected static Nested resolveNested(QueryShardContext context, String nestedPath, QueryBuilder nestedFilter) throws IOException {
        NestedSort nestedSort = new NestedSort();
        nestedSort.setPath(nestedPath);
        nestedSort.setFilter(nestedFilter);

        return resolveNested(context, nestedSort);
    }

    protected static Nested resolveNested(QueryShardContext context, NestedSort nestedSort) throws IOException {
        return resolveNested(context, nestedSort, null);
    }

    protected static Nested resolveNested(QueryShardContext context, NestedSort nestedSort, Nested nested) throws IOException {
        if (nestedSort == null || nestedSort.getPath() == null) {
            return null;
        }

        String nestedPath = nestedSort.getPath();
        QueryBuilder nestedFilter = nestedSort.getFilter();
        NestedSort nestedNestedSort = nestedSort.getNestedSort();

        // verify our nested path
        ObjectMapper nestedObjectMapper = context.getObjectMapper(nestedPath);

        if (nestedObjectMapper == null) {
            throw new QueryShardException(context, "[nested] failed to find nested object under path [" + nestedPath + "]");
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new QueryShardException(context, "[nested] nested object under path [" + nestedPath + "] is not of nested type");
        }

        // get our parent query which will determines our parent documents
        Query parentQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentQuery = Queries.newNonNestedFilter();
        } else {
            parentQuery = objectMapper.nestedTypeFilter();
        }

        // get our child query, potentially applying a users filter
        Query childQuery;
        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            if (nestedFilter != null) {
                assert nestedFilter == Rewriteable.rewrite(nestedFilter, context) : "nested filter is not rewritten";
                if (nested == null) {
                    // this is for back-compat, original single level nested sorting never applied a nested type filter
                    childQuery = nestedFilter.toFilter(context);
                } else {
                    childQuery = Queries.filtered(nestedObjectMapper.nestedTypeFilter(), nestedFilter.toFilter(context));
                }
            } else {
                childQuery = nestedObjectMapper.nestedTypeFilter();
            }
        } finally {
            context.nestedScope().previousLevel();
        }

        // apply filters from the previous nested level
        if (nested != null) {
            parentQuery = Queries.filtered(parentQuery,
                new ToParentBlockJoinQuery(nested.getInnerQuery(), nested.getRootFilter(), ScoreMode.None));

            if (objectMapper != null) {
                childQuery = Queries.filtered(childQuery,
                    new ToChildBlockJoinQuery(nested.getInnerQuery(), context.bitsetFilter(objectMapper.nestedTypeFilter())));
            }
        }

        // wrap up our parent and child and either process the next level of nesting or return
        final Nested innerNested = new Nested(context.bitsetFilter(parentQuery), childQuery);
        if (nestedNestedSort != null) {
            try {
                context.nestedScope().nextLevel(nestedObjectMapper);
                return resolveNested(context, nestedNestedSort, innerNested);
            } finally {
                context.nestedScope().previousLevel();
            }
        } else {
            return innerNested;
        }
    }

    protected static QueryBuilder parseNestedFilter(XContentParser parser) {
        try {
            return parseInnerQueryBuilder(parser);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), "Expected " + NESTED_FILTER_FIELD.getPreferredName() + " element.", e);
        }
    }

    @FunctionalInterface
    private interface Parser<T extends SortBuilder<?>> {
        T fromXContent(XContentParser parser, String elementName) throws IOException;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
