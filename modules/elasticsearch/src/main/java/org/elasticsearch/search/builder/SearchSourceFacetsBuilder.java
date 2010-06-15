/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.builder;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.search.facets.histogram.HistogramFacet;
import org.elasticsearch.search.facets.histogram.HistogramFacetCollectorParser;
import org.elasticsearch.search.facets.query.QueryFacetCollectorParser;
import org.elasticsearch.search.facets.statistical.StatisticalFacetCollectorParser;
import org.elasticsearch.search.facets.terms.TermsFacetCollectorParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * A search source facets builder.
 *
 * @author kimchy (shay.banon)
 * @see SearchSourceBuilder#facets(SearchSourceFacetsBuilder)
 */
public class SearchSourceFacetsBuilder implements ToXContent {

    private List<BuilderQueryFacet> queryFacets;
    private List<BuilderTermsFacet> termsFacets;
    private List<BuilderStatisticalFacet> statisticalFacets;
    private List<BuilderHistogramFacet> histogramFacets;

    public SearchSourceFacetsBuilder queryFacet(String name, XContentQueryBuilder query) {
        return queryFacet(name, query, null);
    }

    /**
     * Adds a query facet (which results in a count facet returned).
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     */
    public SearchSourceFacetsBuilder queryFacet(String name, XContentQueryBuilder query, @Nullable XContentFilterBuilder filter) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new BuilderQueryFacet(name, query, filter, false));
        return this;
    }

    public SearchSourceFacetsBuilder queryFacetGlobal(String name, XContentQueryBuilder query) {
        return queryFacetGlobal(name, query, null);
    }

    /**
     * Adds a query facet (which results in a count facet returned) with an option to
     * be global on the index or bounded by the search query.
     *
     * @param name  The logical name of the facet, it will be returned under the name
     * @param query The query facet
     */
    public SearchSourceFacetsBuilder queryFacetGlobal(String name, XContentQueryBuilder query, @Nullable XContentFilterBuilder filter) {
        if (queryFacets == null) {
            queryFacets = newArrayListWithCapacity(2);
        }
        queryFacets.add(new BuilderQueryFacet(name, query, filter, true));
        return this;
    }

    public SearchSourceFacetsBuilder termsFacet(String name, String fieldName, int size) {
        return termsFacet(name, fieldName, size, null);
    }

    public SearchSourceFacetsBuilder termsFacet(String name, String fieldName, int size, @Nullable XContentFilterBuilder filter) {
        if (termsFacets == null) {
            termsFacets = newArrayListWithCapacity(2);
        }
        termsFacets.add(new BuilderTermsFacet(name, fieldName, size, filter, false));
        return this;
    }

    public SearchSourceFacetsBuilder termsFacetGlobal(String name, String fieldName, int size) {
        return termsFacetGlobal(name, fieldName, size, null);
    }

    public SearchSourceFacetsBuilder termsFacetGlobal(String name, String fieldName, int size, @Nullable XContentFilterBuilder filter) {
        if (termsFacets == null) {
            termsFacets = newArrayListWithCapacity(2);
        }
        termsFacets.add(new BuilderTermsFacet(name, fieldName, size, filter, true));
        return this;
    }

    public SearchSourceFacetsBuilder statisticalFacet(String name, String fieldName) {
        return statisticalFacet(name, fieldName, null);
    }

    public SearchSourceFacetsBuilder statisticalFacet(String name, String fieldName, @Nullable XContentFilterBuilder filter) {
        if (statisticalFacets == null) {
            statisticalFacets = newArrayListWithCapacity(2);
        }
        statisticalFacets.add(new BuilderStatisticalFacet(name, fieldName, filter, false));
        return this;
    }

    public SearchSourceFacetsBuilder statisticalFacetGlobal(String name, String fieldName) {
        return statisticalFacetGlobal(name, fieldName, null);
    }

    public SearchSourceFacetsBuilder statisticalFacetGlobal(String name, String fieldName, @Nullable XContentFilterBuilder filter) {
        if (statisticalFacets == null) {
            statisticalFacets = newArrayListWithCapacity(2);
        }
        statisticalFacets.add(new BuilderStatisticalFacet(name, fieldName, filter, true));
        return this;
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String fieldName, long interval) {
        return histogramFacet(name, fieldName, interval, HistogramFacet.ComparatorType.KEY, null);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String keyFieldName, String valueFieldName, long interval) {
        return histogramFacet(name, keyFieldName, valueFieldName, interval, HistogramFacet.ComparatorType.KEY, null);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String fieldName, long interval, @Nullable XContentFilterBuilder filter) {
        return histogramFacet(name, fieldName, interval, HistogramFacet.ComparatorType.KEY, filter);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String keyFieldName, String valueFieldName, long interval, @Nullable XContentFilterBuilder filter) {
        return histogramFacet(name, keyFieldName, valueFieldName, interval, HistogramFacet.ComparatorType.KEY, filter);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        return histogramFacet(name, fieldName, interval, comparatorType, null);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        return histogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType, null);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String keyFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                    @Nullable XContentFilterBuilder filter) {
        return histogramFacet(name, keyFieldName, null, interval, comparatorType, filter);
    }

    public SearchSourceFacetsBuilder histogramFacet(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                    @Nullable XContentFilterBuilder filter) {
        if (histogramFacets == null) {
            histogramFacets = newArrayListWithCapacity(2);
        }
        histogramFacets.add(new BuilderHistogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType, filter, false));
        return this;
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String fieldName, long interval) {
        return histogramFacet(name, fieldName, interval, HistogramFacet.ComparatorType.KEY, null);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String keyFieldName, String valueFieldName, long interval) {
        return histogramFacet(name, keyFieldName, valueFieldName, interval, HistogramFacet.ComparatorType.KEY, null);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String fieldName, long interval, @Nullable XContentFilterBuilder filter) {
        return histogramFacet(name, fieldName, interval, HistogramFacet.ComparatorType.KEY, filter);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String keyFieldName, String valueFieldName, long interval, @Nullable XContentFilterBuilder filter) {
        return histogramFacet(name, keyFieldName, valueFieldName, interval, HistogramFacet.ComparatorType.KEY, filter);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        return histogramFacetGlobal(name, fieldName, interval, comparatorType, null);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType) {
        return histogramFacetGlobal(name, keyFieldName, valueFieldName, interval, comparatorType, null);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                          @Nullable XContentFilterBuilder filter) {
        return histogramFacetGlobal(name, fieldName, null, interval, comparatorType, filter);
    }

    public SearchSourceFacetsBuilder histogramFacetGlobal(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                                          @Nullable XContentFilterBuilder filter) {
        if (histogramFacets == null) {
            histogramFacets = newArrayListWithCapacity(2);
        }
        histogramFacets.add(new BuilderHistogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType, filter, true));
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (queryFacets == null && termsFacets == null && statisticalFacets == null && histogramFacets == null) {
            return;
        }
        builder.field("facets");

        builder.startObject();

        if (queryFacets != null) {
            for (BuilderQueryFacet queryFacet : queryFacets) {
                builder.startObject(queryFacet.name());
                builder.field(QueryFacetCollectorParser.NAME);
                queryFacet.queryBuilder().toXContent(builder, params);

                if (queryFacet.filter() != null) {
                    builder.field("filter");
                    queryFacet.filter().toXContent(builder, params);
                }

                if (queryFacet.global() != null) {
                    builder.field("global", queryFacet.global());
                }
                builder.endObject();
            }
        }
        if (termsFacets != null) {
            for (BuilderTermsFacet termsFacet : termsFacets) {
                builder.startObject(termsFacet.name());

                builder.startObject(TermsFacetCollectorParser.NAME);
                builder.field("field", termsFacet.fieldName());
                builder.field("size", termsFacet.size());
                builder.endObject();

                if (termsFacet.filter() != null) {
                    builder.field("filter");
                    termsFacet.filter().toXContent(builder, params);
                }
                if (termsFacet.global() != null) {
                    builder.field("global", termsFacet.global());
                }

                builder.endObject();
            }
        }

        if (statisticalFacets != null) {
            for (BuilderStatisticalFacet statisticalFacet : statisticalFacets) {
                builder.startObject(statisticalFacet.name());

                builder.startObject(StatisticalFacetCollectorParser.NAME);
                builder.field("field", statisticalFacet.fieldName());
                builder.endObject();

                if (statisticalFacet.filter() != null) {
                    builder.field("filter");
                    statisticalFacet.filter().toXContent(builder, params);
                }

                if (statisticalFacet.global() != null) {
                    builder.field("global", statisticalFacet.global());
                }

                builder.endObject();
            }
        }

        if (histogramFacets != null) {
            for (BuilderHistogramFacet histogramFacet : histogramFacets) {
                builder.startObject(histogramFacet.name());

                builder.startObject(HistogramFacetCollectorParser.NAME);
                if (histogramFacet.valueFieldName() != null && !histogramFacet.keyFieldName().equals(histogramFacet.valueFieldName())) {
                    builder.field("key_field", histogramFacet.keyFieldName());
                    builder.field("value_field", histogramFacet.valueFieldName());
                } else {
                    builder.field("field", histogramFacet.keyFieldName());
                }
                builder.field("interval", histogramFacet.interval());
                builder.field("comparator", histogramFacet.comparatorType().description());
                builder.endObject();

                if (histogramFacet.filter() != null) {
                    builder.field("filter");
                    histogramFacet.filter().toXContent(builder, params);
                }

                if (histogramFacet.global() != null) {
                    builder.field("global", histogramFacet.global());
                }

                builder.endObject();
            }
        }

        builder.endObject();
    }

    private static class BuilderTermsFacet {
        private final String name;
        private final String fieldName;
        private final int size;
        private final Boolean global;
        private final XContentFilterBuilder filter;

        private BuilderTermsFacet(String name, String fieldName, int size, XContentFilterBuilder filter, Boolean global) {
            this.name = name;
            this.fieldName = fieldName;
            this.size = size;
            this.filter = filter;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public String fieldName() {
            return fieldName;
        }

        public int size() {
            return size;
        }

        public XContentFilterBuilder filter() {
            return filter;
        }

        public Boolean global() {
            return global;
        }
    }

    private static class BuilderQueryFacet {
        private final String name;
        private final XContentQueryBuilder queryBuilder;
        private final XContentFilterBuilder filter;
        private final Boolean global;

        private BuilderQueryFacet(String name, XContentQueryBuilder queryBuilder, XContentFilterBuilder filter, Boolean global) {
            this.name = name;
            this.queryBuilder = queryBuilder;
            this.filter = filter;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public XContentQueryBuilder queryBuilder() {
            return queryBuilder;
        }

        public XContentFilterBuilder filter() {
            return filter;
        }

        public Boolean global() {
            return this.global;
        }
    }

    private static class BuilderStatisticalFacet {
        private final String name;
        private final String fieldName;
        private final XContentFilterBuilder filter;
        private final Boolean global;

        private BuilderStatisticalFacet(String name, String fieldName, XContentFilterBuilder filter, Boolean global) {
            this.name = name;
            this.fieldName = fieldName;
            this.filter = filter;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public String fieldName() {
            return fieldName;
        }

        public XContentFilterBuilder filter() {
            return this.filter;
        }

        public Boolean global() {
            return this.global;
        }
    }

    private static class BuilderHistogramFacet {
        private final String name;
        private final String keyFieldName;
        private final String valueFieldName;
        private final long interval;
        private final HistogramFacet.ComparatorType comparatorType;
        private final XContentFilterBuilder filter;
        private final Boolean global;

        private BuilderHistogramFacet(String name, String keyFieldName, String valueFieldName, long interval, XContentFilterBuilder filter, Boolean global) {
            this(name, keyFieldName, valueFieldName, interval, HistogramFacet.ComparatorType.KEY, filter, global);
        }

        private BuilderHistogramFacet(String name, String keyFieldName, String valueFieldName, long interval, HistogramFacet.ComparatorType comparatorType,
                                      XContentFilterBuilder filter, Boolean global) {
            this.name = name;
            this.keyFieldName = keyFieldName;
            this.valueFieldName = valueFieldName;
            this.interval = interval;
            this.comparatorType = comparatorType;
            this.filter = filter;
            this.global = global;
        }

        public String name() {
            return name;
        }

        public String keyFieldName() {
            return keyFieldName;
        }

        public String valueFieldName() {
            return valueFieldName;
        }

        public long interval() {
            return this.interval;
        }

        public HistogramFacet.ComparatorType comparatorType() {
            return this.comparatorType;
        }

        public XContentFilterBuilder filter() {
            return this.filter;
        }

        public Boolean global() {
            return this.global;
        }
    }
}
