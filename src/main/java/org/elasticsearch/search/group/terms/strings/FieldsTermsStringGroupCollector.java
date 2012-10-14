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
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.group.AbstractGroupCollector;
import org.elasticsearch.search.group.Group;
import org.elasticsearch.search.group.GroupPhaseExecutionException;
import org.elasticsearch.search.group.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class FieldsTermsStringGroupCollector extends AbstractGroupCollector {

    private final FieldDataCache fieldDataCache;

    private final String[] indexFieldsNames;

    private final InternalStringTermsGroup.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType[] fieldsDataType;

    private FieldData[] fieldsData;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public FieldsTermsStringGroupCollector(String groupName, String[] fieldsNames, int size, InternalStringTermsGroup.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                           ImmutableSet<String> excluded, Pattern pattern, String scriptLang, String script, Map<String, Object> params) {
        super(groupName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

        fieldsDataType = new FieldDataType[fieldsNames.length];
        fieldsData = new FieldData[fieldsNames.length];
        indexFieldsNames = new String[fieldsNames.length];

        for (int i = 0; i < fieldsNames.length; i++) {
            MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldsNames[i]);
            if (smartMappers == null || !smartMappers.hasMapper()) {
                this.indexFieldsNames[i] = fieldsNames[i];
                this.fieldsDataType[i] = FieldDataType.DefaultTypes.STRING;
            } else {
                this.indexFieldsNames[i] = smartMappers.mapper().names().indexName();
                this.fieldsDataType[i] = smartMappers.mapper().fieldDataType();
            }

        }

        if (script != null) {
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        } else {
            this.script = null;
        }

        if (excluded.isEmpty() && pattern == null && this.script == null) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.<String>popObjectIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.<String>popObjectIntMap(), excluded, pattern, this.script);
        }

        if (allTerms) {
            try {
                for (int i = 0; i < fieldsNames.length; i++) {
                    for (IndexReader reader : context.searcher().subReaders()) {
                        FieldData fieldData = fieldDataCache.cache(fieldsDataType[i], reader, indexFieldsNames[i]);
                        fieldData.forEachValue(aggregator);
                    }
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
        for (int i = 0; i < indexFieldsNames.length; i++) {
            fieldsData[i] = fieldDataCache.cache(fieldsDataType[i], reader, indexFieldsNames[i]);
        }
        if (script != null) {
            script.setNextReader(reader);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        for (FieldData fieldData : fieldsData) {
            fieldData.forEachValueInDoc(doc, aggregator);
        }
    }

    @Override
    public Group group() {
        TObjectIntHashMap<String> groups = aggregator.groups();
        if (groups.isEmpty()) {
            CacheRecycler.pushObjectIntMap(groups);
            return new InternalStringTermsGroup(groupName, comparatorType, size, ImmutableList.<InternalStringTermsGroup.ScoreDocsEntry>of(), aggregator.missing(), aggregator.total());
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TObjectIntIterator<String> it = groups.iterator(); it.hasNext(); ) {
                    it.advance();
//                    ordered.insertWithOverflow(new InternalStringTermsGroup.StringEntry(it.key(), it.value()));
                }
                InternalStringTermsGroup.ScoreDocsEntry[] list = new InternalStringTermsGroup.ScoreDocsEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsGroup.ScoreDocsEntry) ordered.pop());
                }
                CacheRecycler.pushObjectIntMap(groups);
                return new InternalStringTermsGroup(groupName, comparatorType, size, Arrays.asList(list), aggregator.missing(), aggregator.total());
            } else {
                BoundedTreeSet<InternalStringTermsGroup.ScoreDocsEntry> ordered = new BoundedTreeSet<InternalStringTermsGroup.ScoreDocsEntry>(comparatorType.comparator(), size);
                for (TObjectIntIterator<String> it = groups.iterator(); it.hasNext(); ) {
                    it.advance();
//                    ordered.add(new InternalStringTermsGroup.StringEntry(it.key(), it.value()));
                }
                CacheRecycler.pushObjectIntMap(groups);
                return new InternalStringTermsGroup(groupName, comparatorType, size, ordered, aggregator.missing(), aggregator.total());
            }
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final ImmutableSet<String> excluded;

        private final Matcher matcher;

        private final SearchScript script;

        public AggregatorValueProc(TObjectIntHashMap<String> gruups, ImmutableSet<String> excluded, Pattern pattern, SearchScript script) {
            super(gruups);
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

        private final TObjectIntHashMap<String> groups;

        private int missing;
        private int total;

        public StaticAggregatorValueProc(TObjectIntHashMap<String> groups) {
            this.groups = groups;
        }

        @Override
        public void onValue(String value) {
            groups.putIfAbsent(value, 0);
        }

        @Override
        public void onValue(int docId, String value) {
            groups.adjustOrPutValue(value, 1, 1);
            total++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final TObjectIntHashMap<String> groups() {
            return groups;
        }

        public final int missing() {
            return this.missing;
        }

        public final int total() {
            return this.total;
        }
    }

}
