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

package org.elasticsearch.search.aggregations.metrics.tophits;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext.FieldDataField;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class TopHitsAggregator extends MetricsAggregator {

    /** Simple wrapper around a top-level collector and the current leaf collector. */
    private static class TopDocsAndLeafCollector {
        final TopDocsCollector<?> topLevelCollector;
        LeafCollector leafCollector;

        TopDocsAndLeafCollector(TopDocsCollector<?> topLevelCollector) {
            this.topLevelCollector = topLevelCollector;
        }
    }

    final FetchPhase fetchPhase;
    final SubSearchContext subSearchContext;
    final LongObjectPagedHashMap<TopDocsAndLeafCollector> topDocsCollectors;

    public TopHitsAggregator(FetchPhase fetchPhase, SubSearchContext subSearchContext, String name, AggregationContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.fetchPhase = fetchPhase;
        topDocsCollectors = new LongObjectPagedHashMap<>(1, context.bigArrays());
        this.subSearchContext = subSearchContext;
    }

    @Override
    public boolean needsScores() {
        Sort sort = subSearchContext.sort();
        if (sort != null) {
            return sort.needsScores() || subSearchContext.trackScores();
        } else {
            // sort by score
            return true;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {

        for (LongObjectPagedHashMap.Cursor<TopDocsAndLeafCollector> cursor : topDocsCollectors) {
            cursor.value.leafCollector = cursor.value.topLevelCollector.getLeafCollector(ctx);
        }

        return new LeafBucketCollectorBase(sub, null) {

            Scorer scorer;

            @Override
            public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
                for (LongObjectPagedHashMap.Cursor<TopDocsAndLeafCollector> cursor : topDocsCollectors) {
                    cursor.value.leafCollector.setScorer(scorer);
                }
                super.setScorer(scorer);
            }

            @Override
            public void collect(int docId, long bucket) throws IOException {
                TopDocsAndLeafCollector collectors = topDocsCollectors.get(bucket);
                if (collectors == null) {
                    Sort sort = subSearchContext.sort();
                    int topN = subSearchContext.from() + subSearchContext.size();
                    // In the QueryPhase we don't need this protection, because it is build into the IndexSearcher,
                    // but here we create collectors ourselves and we need prevent OOM because of crazy an offset and size.
                    topN = Math.min(topN, subSearchContext.searcher().getIndexReader().maxDoc());
                    TopDocsCollector<?> topLevelCollector = sort != null ? TopFieldCollector.create(sort, topN, true, subSearchContext.trackScores(), subSearchContext.trackScores()) : TopScoreDocCollector.create(topN);
                    collectors = new TopDocsAndLeafCollector(topLevelCollector);
                    collectors.leafCollector = collectors.topLevelCollector.getLeafCollector(ctx);
                    collectors.leafCollector.setScorer(scorer);
                    topDocsCollectors.put(bucket, collectors);
                }
                collectors.leafCollector.collect(docId);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TopDocsAndLeafCollector topDocsCollector = topDocsCollectors.get(owningBucketOrdinal);
        final InternalTopHits topHits;
        if (topDocsCollector == null) {
            topHits = buildEmptyAggregation();
        } else {
            final TopDocs topDocs = topDocsCollector.topLevelCollector.topDocs();

            subSearchContext.queryResult().topDocs(topDocs);
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            subSearchContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            fetchPhase.execute(subSearchContext);
            FetchSearchResult fetchResult = subSearchContext.fetchResult();
            InternalSearchHit[] internalHits = fetchResult.fetchResult().hits().internalHits();
            for (int i = 0; i < internalHits.length; i++) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                InternalSearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(subSearchContext.shardTarget());
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields);
                }
            }
            topHits = new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, fetchResult.hits(), pipelineAggregators(),
                    metaData());
        }
        return topHits;
    }

    @Override
    public InternalTopHits buildEmptyAggregation() {
        TopDocs topDocs;
        if (subSearchContext.sort() != null) {
            topDocs = new TopFieldDocs(0, new FieldDoc[0], subSearchContext.sort().getSort(), Float.NaN);
        } else {
            topDocs = Lucene.EMPTY_TOP_DOCS;
        }
        return new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, InternalSearchHits.empty(), pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(topDocsCollectors);
    }

    public static class Factory extends AggregatorFactory<Factory> {

        private static final SortParseElement sortParseElement = new SortParseElement();
        private int from = 0;
        private int size = 3;
        private boolean explain = false;
        private boolean version = false;
        private boolean trackScores = false;
        private List<BytesReference> sorts = null;
        private HighlightBuilder highlightBuilder;
        private List<String> fieldNames;
        private List<String> fieldDataFields;
        private List<ScriptField> scriptFields;
        private FetchSourceContext fetchSourceContext;

        public Factory(String name) {
            super(name, InternalTopHits.TYPE);
        }

        /**
         * From index to start the search from. Defaults to <tt>0</tt>.
         */
        public Factory from(int from) {
            this.from = from;
            return this;
        }

        /**
         * Gets the from index to start the search from.
         **/
        public int from() {
            return from;
        }

        /**
         * The number of search hits to return. Defaults to <tt>10</tt>.
         */
        public Factory size(int size) {
            this.size = size;
            return this;
        }

        /**
         * Gets the number of search hits to return.
         */
        public int size() {
            return size;
        }

        /**
         * Adds a sort against the given field name and the sort ordering.
         *
         * @param name
         *            The name of the field
         * @param order
         *            The sort ordering
         */
        public Factory sort(String name, SortOrder order) {
            sort(SortBuilders.fieldSort(name).order(order));
            return this;
        }

        /**
         * Add a sort against the given field name.
         *
         * @param name
         *            The name of the field to sort by
         */
        public Factory sort(String name) {
            sort(SortBuilders.fieldSort(name));
            return this;
        }

        /**
         * Adds a sort builder.
         */
        public Factory sort(SortBuilder sort) {
            try {
                if (sorts == null) {
                    sorts = new ArrayList<>();
                }
                // NORELEASE when sort has been refactored and made writeable
                // add the sortBuilcer to the List directly instead of
                // serialising to XContent
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                sort.toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                sorts.add(builder.bytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        /**
         * Adds a sort builder.
         */
        public Factory sorts(List<BytesReference> sorts) {
            if (this.sorts == null) {
                this.sorts = new ArrayList<>();
            }
            for (BytesReference sort : sorts) {
                this.sorts.add(sort);
            }
            return this;
        }

        /**
         * Gets the bytes representing the sort builders for this request.
         */
        public List<BytesReference> sorts() {
            return sorts;
        }

        /**
         * Adds highlight to perform as part of the search.
         */
        public Factory highlighter(HighlightBuilder highlightBuilder) {
            this.highlightBuilder = highlightBuilder;
            return this;
        }

        /**
         * Gets the hightlighter builder for this request.
         */
        public HighlightBuilder highlighter() {
            return highlightBuilder;
        }

        /**
         * Indicates whether the response should contain the stored _source for
         * every hit
         */
        public Factory fetchSource(boolean fetch) {
            if (this.fetchSourceContext == null) {
                this.fetchSourceContext = new FetchSourceContext(fetch);
            } else {
                this.fetchSourceContext.fetchSource(fetch);
            }
            return this;
        }

        /**
         * Indicate that _source should be returned with every hit, with an
         * "include" and/or "exclude" set which can include simple wildcard
         * elements.
         *
         * @param include
         *            An optional include (optionally wildcarded) pattern to
         *            filter the returned _source
         * @param exclude
         *            An optional exclude (optionally wildcarded) pattern to
         *            filter the returned _source
         */
        public Factory fetchSource(@Nullable String include, @Nullable String exclude) {
            fetchSource(include == null ? Strings.EMPTY_ARRAY : new String[] { include },
                    exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude });
            return this;
        }

        /**
         * Indicate that _source should be returned with every hit, with an
         * "include" and/or "exclude" set which can include simple wildcard
         * elements.
         *
         * @param includes
         *            An optional list of include (optionally wildcarded)
         *            pattern to filter the returned _source
         * @param excludes
         *            An optional list of exclude (optionally wildcarded)
         *            pattern to filter the returned _source
         */
        public Factory fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
            fetchSourceContext = new FetchSourceContext(includes, excludes);
            return this;
        }

        /**
         * Indicate how the _source should be fetched.
         */
        public Factory fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        /**
         * Gets the {@link FetchSourceContext} which defines how the _source
         * should be fetched.
         */
        public FetchSourceContext fetchSource() {
            return fetchSourceContext;
        }

        /**
         * Adds a field to load and return (note, it must be stored) as part of
         * the search request. If none are specified, the source of the document
         * will be return.
         */
        public Factory field(String name) {
            if (fieldNames == null) {
                fieldNames = new ArrayList<>();
            }
            fieldNames.add(name);
            return this;
        }

        /**
         * Sets the fields to load and return as part of the search request. If
         * none are specified, the source of the document will be returned.
         */
        public Factory fields(List<String> fields) {
            this.fieldNames = fields;
            return this;
        }

        /**
         * Sets no fields to be loaded, resulting in only id and type to be
         * returned per field.
         */
        public Factory noFields() {
            this.fieldNames = Collections.emptyList();
            return this;
        }

        /**
         * Gets the fields to load and return as part of the search request.
         */
        public List<String> fields() {
            return fieldNames;
        }

        /**
         * Adds a field to load from the field data cache and return as part of
         * the search request.
         */
        public Factory fieldDataField(String name) {
            if (fieldDataFields == null) {
                fieldDataFields = new ArrayList<>();
            }
            fieldDataFields.add(name);
            return this;
        }

        /**
         * Adds fields to load from the field data cache and return as part of
         * the search request.
         */
        public Factory fieldDataFields(List<String> names) {
            if (fieldDataFields == null) {
                fieldDataFields = new ArrayList<>();
            }
            fieldDataFields.addAll(names);
            return this;
        }

        /**
         * Gets the field-data fields.
         */
        public List<String> fieldDataFields() {
            return fieldDataFields;
        }

        /**
         * Adds a script field under the given name with the provided script.
         *
         * @param name
         *            The name of the field
         * @param script
         *            The script
         */
        public Factory scriptField(String name, Script script) {
            scriptField(name, script, false);
            return this;
        }

        /**
         * Adds a script field under the given name with the provided script.
         *
         * @param name
         *            The name of the field
         * @param script
         *            The script
         */
        public Factory scriptField(String name, Script script, boolean ignoreFailure) {
            if (scriptFields == null) {
                scriptFields = new ArrayList<>();
            }
            scriptFields.add(new ScriptField(name, script, ignoreFailure));
            return this;
        }

        public Factory scriptFields(List<ScriptField> scriptFields) {
            if (this.scriptFields == null) {
                this.scriptFields = new ArrayList<>();
            }
            this.scriptFields.addAll(scriptFields);
            return this;
        }

        /**
         * Gets the script fields.
         */
        public List<ScriptField> scriptFields() {
            return scriptFields;
        }

        /**
         * Should each {@link org.elasticsearch.search.SearchHit} be returned
         * with an explanation of the hit (ranking).
         */
        public Factory explain(boolean explain) {
            this.explain = explain;
            return this;
        }

        /**
         * Indicates whether each search hit will be returned with an
         * explanation of the hit (ranking)
         */
        public boolean explain() {
            return explain;
        }

        /**
         * Should each {@link org.elasticsearch.search.SearchHit} be returned
         * with a version associated with it.
         */
        public Factory version(boolean version) {
            this.version = version;
            return this;
        }

        /**
         * Indicates whether the document's version will be included in the
         * search hits.
         */
        public boolean version() {
            return version;
        }

        /**
         * Applies when sorting, and controls if scores will be tracked as well.
         * Defaults to <tt>false</tt>.
         */
        public Factory trackScores(boolean trackScores) {
            this.trackScores = trackScores;
            return this;
        }

        /**
         * Indicates whether scores will be tracked for this request.
         */
        public boolean trackScores() {
            return trackScores;
        }

        @Override
        public Aggregator createInternal(AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            SubSearchContext subSearchContext = new SubSearchContext(aggregationContext.searchContext());
            subSearchContext.explain(explain);
            subSearchContext.version(version);
            subSearchContext.trackScores(trackScores);
            subSearchContext.from(from);
            subSearchContext.size(size);
            if (sorts != null) {
                XContentParser completeSortParser = null;
                try {
                    XContentBuilder completeSortBuilder = XContentFactory.jsonBuilder();
                    completeSortBuilder.startObject();
                    completeSortBuilder.startArray("sort");
                    for (BytesReference sort : sorts) {
                        XContentParser parser = XContentFactory.xContent(sort).createParser(sort);
                        parser.nextToken();
                        completeSortBuilder.copyCurrentStructure(parser);
                    }
                    completeSortBuilder.endArray();
                    completeSortBuilder.endObject();
                    BytesReference completeSortBytes = completeSortBuilder.bytes();
                    completeSortParser = XContentFactory.xContent(completeSortBytes).createParser(completeSortBytes);
                    completeSortParser.nextToken();
                    completeSortParser.nextToken();
                    completeSortParser.nextToken();
                    sortParseElement.parse(completeSortParser, subSearchContext);
                } catch (Exception e) {
                    XContentLocation location = completeSortParser != null ? completeSortParser.getTokenLocation() : null;
                    throw new ParsingException(location, "failed to parse sort source in aggregation [" + name + "]", e);
                }
            }
            if (fieldNames != null) {
                subSearchContext.fieldNames().addAll(fieldNames);
            }
            if (fieldDataFields != null) {
                FieldDataFieldsContext fieldDataFieldsContext = subSearchContext
                        .getFetchSubPhaseContext(FieldDataFieldsFetchSubPhase.CONTEXT_FACTORY);
                for (String field : fieldDataFields) {
                    fieldDataFieldsContext.add(new FieldDataField(field));
                }
                fieldDataFieldsContext.setHitExecutionNeeded(true);
            }
            if (scriptFields != null) {
                for (ScriptField field : scriptFields) {
                    SearchScript searchScript = subSearchContext.scriptService().search(subSearchContext.lookup(), field.script(),
                            ScriptContext.Standard.SEARCH, Collections.emptyMap());
                    subSearchContext.scriptFields().add(new org.elasticsearch.search.fetch.script.ScriptFieldsContext.ScriptField(
                            field.fieldName(), searchScript, field.ignoreFailure()));
                }
            }
            if (fetchSourceContext != null) {
                subSearchContext.fetchSourceContext(fetchSourceContext);
            }
            if (highlightBuilder != null) {
                subSearchContext.highlight(highlightBuilder.build(aggregationContext.searchContext().indexShard().getQueryShardContext()));
            }
            return new TopHitsAggregator(aggregationContext.searchContext().fetchPhase(), subSearchContext, name, aggregationContext,
                    parent, pipelineAggregators, metaData);
        }

        @Override
        public Factory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
            builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
            builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
            builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
            if (fetchSourceContext != null) {
                builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext);
            }
            if (fieldNames != null) {
                if (fieldNames.size() == 1) {
                    builder.field(SearchSourceBuilder.FIELDS_FIELD.getPreferredName(), fieldNames.get(0));
                } else {
                    builder.startArray(SearchSourceBuilder.FIELDS_FIELD.getPreferredName());
                    for (String fieldName : fieldNames) {
                        builder.value(fieldName);
                    }
                    builder.endArray();
                }
            }
            if (fieldDataFields != null) {
                builder.startArray(SearchSourceBuilder.FIELDDATA_FIELDS_FIELD.getPreferredName());
                for (String fieldDataField : fieldDataFields) {
                    builder.value(fieldDataField);
                }
                builder.endArray();
            }
            if (scriptFields != null) {
                builder.startObject(SearchSourceBuilder.SCRIPT_FIELDS_FIELD.getPreferredName());
                for (ScriptField scriptField : scriptFields) {
                    scriptField.toXContent(builder, params);
                }
                builder.endObject();
            }
            if (sorts != null) {
                builder.startArray(SearchSourceBuilder.SORT_FIELD.getPreferredName());
                for (BytesReference sort : sorts) {
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(sort);
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                }
                builder.endArray();
            }
            if (trackScores) {
                builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), true);
            }
            if (highlightBuilder != null) {
                this.highlightBuilder.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        protected AggregatorFactory doReadFrom(String name, StreamInput in) throws IOException {
            Factory factory = new Factory(name);
            factory.explain = in.readBoolean();
            factory.fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
            if (in.readBoolean()) {
                int size = in.readVInt();
                List<String> fieldDataFields = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    fieldDataFields.add(in.readString());
                }
                factory.fieldDataFields = fieldDataFields;
            }
            if (in.readBoolean()) {
                int size = in.readVInt();
                List<String> fieldNames = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    fieldNames.add(in.readString());
                }
                factory.fieldNames = fieldNames;
            }
            factory.from = in.readVInt();
            if (in.readBoolean()) {
                factory.highlightBuilder = HighlightBuilder.PROTOTYPE.readFrom(in);
            }
            if (in.readBoolean()) {
                int size = in.readVInt();
                List<ScriptField> scriptFields = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    scriptFields.add(ScriptField.PROTOTYPE.readFrom(in));
                }
                factory.scriptFields = scriptFields;
            }
            factory.size = in.readVInt();
            if (in.readBoolean()) {
                int size = in.readVInt();
                List<BytesReference> sorts = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    sorts.add(in.readBytesReference());
                }
                factory.sorts = sorts;
            }
            factory.trackScores = in.readBoolean();
            factory.version = in.readBoolean();
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeBoolean(explain);
            FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
            boolean hasFieldDataFields = fieldDataFields != null;
            out.writeBoolean(hasFieldDataFields);
            if (hasFieldDataFields) {
                out.writeVInt(fieldDataFields.size());
                for (String fieldName : fieldDataFields) {
                    out.writeString(fieldName);
                }
            }
            boolean hasFieldNames = fieldNames != null;
            out.writeBoolean(hasFieldNames);
            if (hasFieldNames) {
                out.writeVInt(fieldNames.size());
                for (String fieldName : fieldNames) {
                    out.writeString(fieldName);
                }
            }
            out.writeVInt(from);
            boolean hasHighlighter = highlightBuilder != null;
            out.writeBoolean(hasHighlighter);
            if (hasHighlighter) {
                highlightBuilder.writeTo(out);
            }
            boolean hasScriptFields = scriptFields != null;
            out.writeBoolean(hasScriptFields);
            if (hasScriptFields) {
                out.writeVInt(scriptFields.size());
                for (ScriptField scriptField : scriptFields) {
                    scriptField.writeTo(out);
                }
            }
            out.writeVInt(size);
            boolean hasSorts = sorts != null;
            out.writeBoolean(hasSorts);
            if (hasSorts) {
                out.writeVInt(sorts.size());
                for (BytesReference sort : sorts) {
                    out.writeBytesReference(sort);
                }
            }
            out.writeBoolean(trackScores);
            out.writeBoolean(version);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(explain, fetchSourceContext, fieldDataFields, fieldNames, from, highlightBuilder, scriptFields, size, sorts,
                    trackScores, version);
        }

        @Override
        protected boolean doEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(explain, other.explain)
                    && Objects.equals(fetchSourceContext, other.fetchSourceContext)
                    && Objects.equals(fieldDataFields, other.fieldDataFields)
                    && Objects.equals(fieldNames, other.fieldNames)
                    && Objects.equals(from, other.from)
                    && Objects.equals(highlightBuilder, other.highlightBuilder)
                    && Objects.equals(scriptFields, other.scriptFields)
                    && Objects.equals(size, other.size)
                    && Objects.equals(sorts, other.sorts)
                    && Objects.equals(trackScores, other.trackScores)
                    && Objects.equals(version, other.version);
        }
    }
}
