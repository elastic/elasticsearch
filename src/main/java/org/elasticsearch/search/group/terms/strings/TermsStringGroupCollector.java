/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.group.terms.strings;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.group.AbstractGroupCollector;
import org.elasticsearch.search.group.Group;
import org.elasticsearch.search.group.GroupPhaseExecutionException;
import org.elasticsearch.search.group.terms.TermsGroup;
import org.elasticsearch.search.group.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class TermsStringGroupCollector extends AbstractGroupCollector {

    static ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>> cache =
            new ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>>() {
        @Override
        protected ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<java.lang.String>>>(new ArrayDeque<TObjectIntHashMap<String>>());
        }
    };


    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private final TermsGroup.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType fieldDataType;

    private FieldData fieldData;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsStringGroupCollector(String groupName, String fieldName, int size, TermsGroup.ComparatorType comparatorType,
            boolean allTerms, SearchContext context, ImmutableSet<String> excluded, Pattern pattern, String scriptLang,
            String script, Map<String, Object> params) throws IOException
    {
        super(groupName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            this.indexFieldName = fieldName;
            this.fieldDataType = FieldDataType.DefaultTypes.STRING;
        } else {
            // add type filter if there is exact doc mapper associated with it
            if (smartMappers.hasDocMapper()) {
                setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
            }

            this.indexFieldName = smartMappers.mapper().names().indexName();
            this.fieldDataType = smartMappers.mapper().fieldDataType();
        }

        if (script != null) {
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        } else {
            this.script = null;
        }

        if (excluded.isEmpty() && pattern == null && this.script == null) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.<String, Tuple<Integer, List<ScoreDoc>>>popHashMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.<String, Tuple<Integer, List<ScoreDoc>>>popHashMap(), excluded, pattern, this.script);
        }

        if (allTerms) {
            try {
                for (IndexReader reader : context.searcher().subReaders()) {
                    FieldData fieldData = fieldDataCache.cache(fieldDataType, reader, indexFieldName);
                    fieldData.forEachValue(aggregator);
                }
            } catch (Exception e) {
                throw new GroupPhaseExecutionException("failed to load all terms", e);
            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = fieldDataCache.cache(fieldDataType, reader, indexFieldName);
        if (script != null) {
            script.setNextReader(reader);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, aggregator);
    }

    public ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> getDocs() {
        return aggregator.groups;
    }

    @Override
    public Group group() {
        ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> groups = aggregator.groups();
        if (groups.isEmpty()) {
            // TODO: ??
            CacheRecycler.pushHashMap(groups);
            return new InternalStringTermsGroup(groupName, comparatorType, size,
                    ImmutableList.<InternalStringTermsGroup.ScoreDocsEntry> of(), aggregator.missing(), aggregator.total());
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (String term : groups.keySet()) {
                    Tuple<Integer, List<ScoreDoc>> pair = groups.get(term);
                    ordered.insertWithOverflow(new InternalStringTermsGroup.ScoreDocsEntry(term, pair.v1(), pair.v2())); 
                }
                InternalStringTermsGroup.ScoreDocsEntry[] list = new InternalStringTermsGroup.ScoreDocsEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsGroup.ScoreDocsEntry) ordered.pop());
                }
                CacheRecycler.pushHashMap(groups);
                return new InternalStringTermsGroup(groupName, comparatorType, size, Arrays.asList(list), aggregator.missing(), aggregator.total());
            } else {
                BoundedTreeSet<InternalStringTermsGroup.ScoreDocsEntry> ordered = new BoundedTreeSet<InternalStringTermsGroup.ScoreDocsEntry>(
                        comparatorType.comparator(), size);
                for (String term : groups.keySet()) {
                    Tuple<Integer, List<ScoreDoc>> pair = groups.get(term);
                    ordered.add(new InternalStringTermsGroup.ScoreDocsEntry(term, pair.v1(), pair.v2())); 
                }
                CacheRecycler.pushHashMap(groups);
                return new InternalStringTermsGroup(groupName, comparatorType, size, ordered, aggregator.missing(), aggregator.total());
            }
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final ImmutableSet<String> excluded;

        private final Matcher matcher;

        private final SearchScript script;

        public AggregatorValueProc(ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> groups,
                ImmutableSet<String> excluded, Pattern pattern, SearchScript script)
        {
            super(groups);
            this.excluded = excluded;
            this.matcher = pattern != null ? pattern.matcher("") : null;
            this.script = script;
        }

        @Override
        public void onValue(int docId, String value) {
            if (excluded != null && excluded.contains(value)) {
                return;
            }
            if (matcher != null && !matcher.reset(value).matches()) {
                return;
            }
            if (script != null) {
                script.setNextDocId(docId);
                script.setNextVar("term", value);
                Object scriptValue = script.run();
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    value = scriptValue.toString();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc implements FieldData.StringValueInDocProc, FieldData.StringValueProc {

        private final ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> groups;

        private int missing = 0;
        private int total = 0;

        // TODO: correct class for cache?
        public StaticAggregatorValueProc(ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> groups) {
            this.groups = groups;
        }

        @Override
        public void onValue(String value) {
            groups.put(value, Tuple.tuple(0, (List<ScoreDoc>)new ArrayList<ScoreDoc>()));
        }

        @Override
        public void onValue(int docId, String value) {
            if (!groups.containsKey(value)) {
                groups.put(value, Tuple.tuple(0, (List<ScoreDoc>)new ArrayList<ScoreDoc>()));
            }
            Tuple<Integer, List<ScoreDoc>> tuple = groups.get(value);
            List<ScoreDoc> docs = tuple.v2();
            // TODO: score and shard
            docs.add(new ScoreDoc(docId, (float)0.0));
            groups.put(value, Tuple.tuple(tuple.v1() + 1, docs));
            total++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final ExtTHashMap<String, Tuple<Integer, List<ScoreDoc>>> groups() {
            return groups;
        }

        public final int missing() {
            return this.missing;
        }

        public int total() {
            return this.total;
        }
    }

}
