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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Everything used to build and execute aggregations and the
 * {@link ValuesSource data sources} that power them.
 * <p>
 * In production we always use the {@link ProductionAggregationContext} but
 * this is {@code abstract} so that tests can build it without creating the
 * massing {@link QueryShardContext}.
 */
public abstract class AggregationContext {
    /**
     * The query at the top level of the search in which these aggregations are running.
     */
    public abstract Query query();

    /**
     * The time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    public abstract long nowInMillis();

    /**
     * Lookup the context for a field.
     */
    public final FieldContext buildFieldContext(String field) {
        MappedFieldType ft = getFieldType(field);
        if (ft == null) {
            // The field is unmapped
            return null;
        }
        return new FieldContext(field, buildFieldData(ft), ft);
    }

    /**
     * Lookup the context for an already resolved field type.
     */
    public final FieldContext buildFieldContext(MappedFieldType ft) {
        return new FieldContext(ft.name(), buildFieldData(ft), ft);
    }

    /**
     * Build field data.
     */
    protected abstract IndexFieldData<?> buildFieldData(MappedFieldType ft);

    /**
     * Lookup a {@link MappedFieldType} by path.
     */
    public abstract MappedFieldType getFieldType(String path);

    /**
     * Lookup a field {@link Mapper} by path.
     */
    public abstract Mapper getMapper(String path);

    /**
     * Compile a script.
     */
    public abstract <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context);

    /**
     * Fetch the shared {@link SearchLookup}.
     */
    public abstract SearchLookup lookup();

    /**
     * The {@link ValuesSourceRegistry} to resolve {@link Aggregator}s and the like.
     */
    public abstract ValuesSourceRegistry getValuesSourceRegistry();

    /**
     * The {@link AggregationUsageService} used to track which aggregations are
     * actually used.
     */
    public final AggregationUsageService getUsageService() {
        return getValuesSourceRegistry().getUsageService();
    }

    /**
     * Utility to share and track large arrays.
     */
    public abstract BigArrays bigArrays();

    /**
     * The searcher that will execute this query.
     */
    public abstract IndexSearcher searcher();

    /**
     * Build a query.
     */
    public abstract Query buildQuery(QueryBuilder builder) throws IOException;

    /**
     * The settings for the index against which this search is running.
     */
    public abstract IndexSettings getIndexSettings();

    /**
     * Compile a sort.
     */
    public abstract Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException;

    /**
     * Find an {@link ObjectMapper}.
     */
    public abstract ObjectMapper getObjectMapper(String path);

    /**
     * Access the nested scope. Stay away from this unless you are dealing with nested.
     */
    public abstract NestedScope nestedScope();

    /**
     * Implementation of {@linkplain AggregationContext} for production usage
     * that wraps our ubiquitous {@link QueryShardContext} and the top level
     * {@link Query}. Unit tests should avoid using this because it requires
     * a <strong>huge</strong> portion of a real Elasticsearch node.
     */
    public static class ProductionAggregationContext extends AggregationContext {
        private final QueryShardContext context;
        private final Query query;

        public ProductionAggregationContext(QueryShardContext context, Query query) {
            this.context = context;
            this.query = query;
        }

        @Override
        public Query query() {
            return query;
        }

        @Override
        public long nowInMillis() {
            return context.nowInMillis();
        }

        @Override
        protected IndexFieldData<?> buildFieldData(MappedFieldType ft) {
            return context.getForField(ft);
        }

        @Override
        public MappedFieldType getFieldType(String path) {
            return context.getFieldType(path);
        }

        @Override
        public Mapper getMapper(String path) {
            return context.getMapperService().documentMapper().mappers().getMapper(path);
        }

        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> scriptContext) {
            return context.compile(script, scriptContext);
        }

        @Override
        public SearchLookup lookup() {
            return context.lookup();
        }

        @Override
        public ValuesSourceRegistry getValuesSourceRegistry() {
            return context.getValuesSourceRegistry();
        }

        @Override
        public BigArrays bigArrays() {
            return context.bigArrays();
        }

        @Override
        public IndexSearcher searcher() {
            return context.searcher();
        }

        @Override
        public Query buildQuery(QueryBuilder builder) throws IOException {
            return builder.toQuery(context);
        }

        @Override
        public IndexSettings getIndexSettings() {
            return context.getIndexSettings();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders) throws IOException {
            return SortBuilder.buildSort(sortBuilders, context);
        }

        @Override
        public ObjectMapper getObjectMapper(String path) {
            return context.getObjectMapper(path);
        }

        @Override
        public NestedScope nestedScope() {
            return context.nestedScope();
        }
    }
}
