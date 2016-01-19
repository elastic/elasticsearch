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

package org.elasticsearch.index.percolator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolatorService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Each shard will have a percolator registry even if there isn't a {@link PercolatorService#TYPE_NAME} document type in the index.
 * For shards with indices that have no {@link PercolatorService#TYPE_NAME} document type, this will hold no percolate queries.
 * <p>
 * Once a document type has been created, the real-time percolator will start to listen to write events and update the
 * this registry with queries in real time.
 */
public final class PercolatorQueriesRegistry extends AbstractIndexShardComponent implements Closeable {

    public final static Setting<Boolean> INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING = Setting.boolSetting("index.percolator.map_unmapped_fields_as_string", false, false, Setting.Scope.INDEX);

    private final ConcurrentMap<BytesRef, Query> percolateQueries = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final QueryShardContext queryShardContext;
    private boolean mapUnmappedFieldsAsString;
    private final MeanMetric percolateMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();
    private final CounterMetric numberOfQueries = new CounterMetric();

    public PercolatorQueriesRegistry(ShardId shardId, IndexSettings indexSettings, QueryShardContext queryShardContext) {
        super(shardId, indexSettings);
        this.queryShardContext = queryShardContext;
        this.mapUnmappedFieldsAsString = indexSettings.getValue(INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING);
    }

    public ConcurrentMap<BytesRef, Query> getPercolateQueries() {
        return percolateQueries;
    }

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        percolateQueries.clear();
    }


    public void addPercolateQuery(String idAsString, BytesReference source) {
        Query newquery = parsePercolatorDocument(idAsString, source);
        BytesRef id = new BytesRef(idAsString);
        percolateQueries.put(id, newquery);
        numberOfQueries.inc();

    }

    public void removePercolateQuery(String idAsString) {
        BytesRef id = new BytesRef(idAsString);
        Query query = percolateQueries.remove(id);
        if (query != null) {
            numberOfQueries.dec();
        }
    }

    public Query parsePercolatorDocument(String id, BytesReference source) {
        try (XContentParser sourceParser = XContentHelper.createParser(source)) {
            String currentFieldName = null;
            XContentParser.Token token = sourceParser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchException("failed to parse query [" + id + "], not starting with OBJECT");
            }
            while ((token = sourceParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = sourceParser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        return parseQuery(queryShardContext, mapUnmappedFieldsAsString, sourceParser);
                    } else {
                        sourceParser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    sourceParser.skipChildren();
                }
            }
        } catch (Exception e) {
            throw new PercolatorException(shardId().index(), "failed to parse query [" + id + "]", e);
        }
        return null;
    }

    public static Query parseQuery(QueryShardContext queryShardContext, boolean mapUnmappedFieldsAsString, XContentParser parser) {
        QueryShardContext context = new QueryShardContext(queryShardContext);
        try {
            context.reset(parser);
            // This means that fields in the query need to exist in the mapping prior to registering this query
            // The reason that this is required, is that if a field doesn't exist then the query assumes defaults, which may be undesired.
            //
            // Even worse when fields mentioned in percolator queries do go added to map after the queries have been registered
            // then the percolator queries don't work as expected any more.
            //
            // Query parsing can't introduce new fields in mappings (which happens when registering a percolator query),
            // because field type can't be inferred from queries (like document do) so the best option here is to disallow
            // the usage of unmapped fields in percolator queries to avoid unexpected behaviour
            //
            // if index.percolator.map_unmapped_fields_as_string is set to true, query can contain unmapped fields which will be mapped
            // as an analyzed string.
            context.setAllowUnmappedFields(false);
            context.setMapUnmappedFieldAsString(mapUnmappedFieldsAsString);
            return context.parseInnerQuery();
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "Failed to parse", e);
        } finally {
            context.reset(null);
        }
    }

    public void loadQueries(IndexReader reader) {
        logger.trace("loading percolator queries...");
        final int loadedQueries;
        try {
            Query query = new TermQuery(new Term(TypeFieldMapper.NAME, PercolatorService.TYPE_NAME));
            QueriesLoaderCollector queryCollector = new QueriesLoaderCollector(PercolatorQueriesRegistry.this, logger);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            indexSearcher.setQueryCache(null);
            indexSearcher.search(query, queryCollector);
            Map<BytesRef, Query> queries = queryCollector.queries();
            for (Map.Entry<BytesRef, Query> entry : queries.entrySet()) {
                percolateQueries.put(entry.getKey(), entry.getValue());
                numberOfQueries.inc();
            }
            loadedQueries = queries.size();
        } catch (Exception e) {
            throw new PercolatorException(shardId.index(), "failed to load queries from percolator index", e);
        }
        logger.debug("done loading [{}] percolator queries", loadedQueries);
    }

    public boolean isPercolatorQuery(Engine.Index operation) {
        if (PercolatorService.TYPE_NAME.equals(operation.type())) {
            parsePercolatorDocument(operation.id(), operation.source());
            return true;
        }
        return false;
    }

    public boolean isPercolatorQuery(Engine.Delete operation) {
        return PercolatorService.TYPE_NAME.equals(operation.type());
    }

    public synchronized void updatePercolateQuery(Engine engine, String id) {
        // this can be called out of order as long as for every change to a percolator document it's invoked. This will always
        // fetch the latest change but might fetch the same change twice if updates / deletes happen concurrently.
        try (Engine.GetResult getResult = engine.get(new Engine.Get(true, new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(PercolatorService.TYPE_NAME, id))))) {
            if (getResult.exists()) {
                addPercolateQuery(id, getResult.source().source);
            } else {
                removePercolateQuery(id);
            }
        }
    }

    public void prePercolate() {
        currentMetric.inc();
    }

    public void postPercolate(long tookInNanos) {
        currentMetric.dec();
        percolateMetric.inc(tookInNanos);
    }

    /**
     * @return The current metrics
     */
    public PercolateStats stats() {
        return new PercolateStats(percolateMetric.count(), TimeUnit.NANOSECONDS.toMillis(percolateMetric.sum()), currentMetric.count(), -1, numberOfQueries.count());
    }

    // Enable when a more efficient manner is found for estimating the size of a Lucene query.
    /*private static long computeSizeInMemory(HashedBytesRef id, Query query) {
        long size = (3 * RamUsageEstimator.NUM_BYTES_INT) + RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + id.bytes.bytes.length;
        size += RamEstimator.sizeOf(query);
        return size;
    }

    private static final class RamEstimator {
        // we move this into it's own class to exclude it from the forbidden API checks
        // it's fine to use here!
        static long sizeOf(Query query) {
            return RamUsageEstimator.sizeOf(query);
        }
    }*/
}
