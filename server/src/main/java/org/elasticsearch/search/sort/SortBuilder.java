/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.search.sort.NestedSortBuilder.FILTER_FIELD;

public abstract class SortBuilder<T extends SortBuilder<T>> implements NamedWriteable, ToXContentObject, Rewriteable<SortBuilder<?>> {

    protected SortOrder order = SortOrder.ASC;

    // parse fields common to more than one SortBuilder
    public static final ParseField ORDER_FIELD = new ParseField("order");

    private static final Map<String, Parser<?>> PARSERS = Map.of(
            ScriptSortBuilder.NAME, ScriptSortBuilder::fromXContent,
            GeoDistanceSortBuilder.NAME, GeoDistanceSortBuilder::fromXContent,
            GeoDistanceSortBuilder.ALTERNATIVE_NAME, GeoDistanceSortBuilder::fromXContent,
            // TODO: this can deadlock as it might access the ScoreSortBuilder (subclass) initializer from the SortBuilder initializer!!!
            ScoreSortBuilder.NAME, ScoreSortBuilder::fromXContent);

    /**
     * Create a {@linkplain SortFieldAndFormat} from this builder.
     */
    protected abstract SortFieldAndFormat build(SearchExecutionContext context) throws IOException;

    /**
     * Create a {@linkplain BucketedSort} which is useful for sorting inside of aggregations.
     */
    public abstract BucketedSort buildBucketedSort(
        SearchExecutionContext context,
        BigArrays bigArrays,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) throws IOException;

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

    public static Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders, SearchExecutionContext context) throws IOException {
        List<SortField> sortFields = new ArrayList<>(sortBuilders.size());
        List<DocValueFormat> sortFormats = new ArrayList<>(sortBuilders.size());
        for (SortBuilder<?> builder : sortBuilders) {
            SortFieldAndFormat sf = builder.build(context);
            sortFields.add(sf.field);
            sortFormats.add(sf.format);
        }
        if (sortFields.isEmpty() == false) {
            // optimize if we just sort on score non reversed, we don't really
            // need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && sortField.getReverse() == false) {
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

    protected static Nested resolveNested(SearchExecutionContext context, NestedSortBuilder nestedSort) throws IOException {
        final Query childQuery = resolveNestedQuery(context, nestedSort, null);
        if (childQuery == null) {
            return null;
        }
        final NestedObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        final Query parentQuery;
        if (objectMapper == null) {
            parentQuery = Queries.newNonNestedFilter();
        } else {
            parentQuery = objectMapper.nestedTypeFilter();
        }
        return new Nested(context.bitsetFilter(parentQuery), childQuery, nestedSort, context.searcher());
    }

    private static Query resolveNestedQuery(SearchExecutionContext context,
                                            NestedSortBuilder nestedSort,
                                            Query parentQuery) throws IOException {
        if (nestedSort == null || nestedSort.getPath() == null) {
            return null;
        }

        String nestedPath = nestedSort.getPath();
        QueryBuilder nestedFilter = nestedSort.getFilter();
        NestedSortBuilder nestedNestedSort = nestedSort.getNestedSort();

        // verify our nested path
        ObjectMapper objectMapper = context.getObjectMapper(nestedPath);

        if (objectMapper == null) {
            throw new QueryShardException(context, "[nested] failed to find nested object under path [" + nestedPath + "]");
        }
        if (objectMapper.isNested() == false) {
            throw new QueryShardException(context, "[nested] nested object under path [" + nestedPath + "] is not of nested type");
        }
        NestedObjectMapper nestedObjectMapper = (NestedObjectMapper) objectMapper;
        NestedObjectMapper parentMapper = context.nestedScope().getObjectMapper();

        // get our child query, potentially applying a users filter
        Query childQuery;
        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            if (nestedFilter != null) {
                assert nestedFilter == Rewriteable.rewrite(nestedFilter, context) : "nested filter is not rewritten";
                if (parentQuery == null) {
                    // this is for back-compat, original single level nested sorting never applied a nested type filter
                    childQuery = nestedFilter.toQuery(context);
                } else {
                    childQuery = Queries.filtered(nestedObjectMapper.nestedTypeFilter(), nestedFilter.toQuery(context));
                }
            } else {
                childQuery = nestedObjectMapper.nestedTypeFilter();
            }
        } finally {
            context.nestedScope().previousLevel();
        }

        // apply filters from the previous nested level
        if (parentQuery != null) {
            if (parentMapper != null) {
                childQuery = Queries.filtered(childQuery,
                    new ToChildBlockJoinQuery(parentQuery, context.bitsetFilter(parentMapper.nestedTypeFilter())));
            }
        }

        // wrap up our parent and child and either process the next level of nesting or return
        if (nestedNestedSort != null) {
            try {
                context.nestedScope().nextLevel(nestedObjectMapper);
                return resolveNestedQuery(context, nestedNestedSort, childQuery);
            } finally {
                context.nestedScope().previousLevel();
            }
        } else {
            return childQuery;
        }
    }

    protected static QueryBuilder parseNestedFilter(XContentParser parser) {
        try {
            return parseInnerQueryBuilder(parser);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), "Expected " + FILTER_FIELD.getPreferredName() + " element.", e);
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
