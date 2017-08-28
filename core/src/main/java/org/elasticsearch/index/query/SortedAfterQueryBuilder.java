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

import org.apache.lucene.queries.SearchAfterSortedDocQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link QueryBuilder} that only matches documents which sort fields are greater than the provided values.
 * This works only if the index is sorted and the provided values match the sort field types.
 */
public class SortedAfterQueryBuilder extends AbstractQueryBuilder<SortedAfterQueryBuilder> {
    public static final String NAME = "sorted_after";

    private static final ObjectParser<SortedAfterQueryBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(NAME);
        PARSER.declareField(SortedAfterQueryBuilder::setSortValues, (parser, context) -> SearchAfterBuilder.fromXContent(parser),
            new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER.declareString(SortedAfterQueryBuilder::queryName, AbstractQueryBuilder.NAME_FIELD);
        PARSER.declareFloat(SortedAfterQueryBuilder::boost, AbstractQueryBuilder.BOOST_FIELD);
    }
    public static SortedAfterQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SortedAfterQueryBuilder(), null);
    }

    private final SearchAfterBuilder builder;

    public SortedAfterQueryBuilder() {
        this.builder = new SearchAfterBuilder();
    }

    public SortedAfterQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.builder = new SearchAfterBuilder(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        builder.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return "sorted_after";
    }

    public SortedAfterQueryBuilder setSortValues(Object[] values) {
        builder.setSortValues(values);
        return this;
    }

    SortedAfterQueryBuilder setSortValues(SearchAfterBuilder other) {
        builder.setSortValues(other.getSortValues());
        return this;
    }

    public Object[] getSortValues() {
        return builder.getSortValues();
    }

    @Override
    public Query doToQuery(QueryShardContext context) throws IOException {
        IndexSortConfig indexSortConfig = context.getMapperService().getIndexSettings().getIndexSortConfig();
        if (indexSortConfig.hasIndexSort() == false) {
            throw new IllegalArgumentException("[after] query cannot be applied on non-sorted index [" + context.index().getName() + "]");
        }
        Sort indexSort = indexSortConfig.buildIndexSort(context.getMapperService()::fullName, context::getForField);
        DocValueFormat[] docValueFormats= new DocValueFormat[indexSort.getSort().length];
        for (int i = 0; i < docValueFormats.length; i++) {
            SortField sortField = indexSort.getSort()[i];
            MappedFieldType ft = context.getMapperService().fullName(sortField.getField());
            assert (ft != null);
            docValueFormats[i] = ft.docValueFormat(null, null);
        }
        FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(new SortAndFormats(indexSort, docValueFormats), builder.getSortValues());
        return new SearchAfterSortedDocQuery(indexSort, fieldDoc);
    }

    @Override
    protected boolean doEquals(SortedAfterQueryBuilder other) {
        return Objects.equals(builder, other.builder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(getClass(), builder);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.array("values", this.builder.getSortValues());
        printBoostAndQueryName(builder);
        builder.endObject();
    }
}
