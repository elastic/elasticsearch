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

package org.elasticsearch.index.termvectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.TermVectorsFilter;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.termvectors.dfs.DfsOnlyRequest;
import org.elasticsearch.action.termvectors.dfs.DfsOnlyResponse;
import org.elasticsearch.action.termvectors.dfs.TransportDfsOnlyAction;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 */

public class ShardTermVectorsService extends AbstractIndexShardComponent {

    private IndexShard indexShard;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final TransportDfsOnlyAction dfsAction;

    @Inject
    public ShardTermVectorsService(ShardId shardId, @IndexSettings Settings indexSettings, MappingUpdatedAction mappingUpdatedAction, TransportDfsOnlyAction dfsAction) {
        super(shardId, indexSettings);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.dfsAction = dfsAction;
    }

    // sadly, to overcome cyclic dep, we need to do this and inject it ourselves...
    public ShardTermVectorsService setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        return this;
    }

    public TermVectorsResponse getTermVectors(TermVectorsRequest request, String concreteIndex) {
        final TermVectorsResponse termVectorsResponse = new TermVectorsResponse(concreteIndex, request.type(), request.id());
        final Term uidTerm = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(request.type(), request.id()));

        Engine.GetResult get = indexShard.get(new Engine.Get(request.realtime(), uidTerm).version(request.version()).versionType(request.versionType()));

        Fields termVectorsByField = null;
        boolean docFromTranslog = get.source() != null;
        AggregatedDfs dfs = null;
        TermVectorsFilter termVectorsFilter = null;

        /* fetched from translog is treated as an artificial document */
        if (docFromTranslog) {
            request.doc(get.source().source, false);
            termVectorsResponse.setDocVersion(get.version());
        }

        /* handle potential wildcards in fields */
        if (request.selectedFields() != null) {
            handleFieldWildcards(request);
        }

        final Engine.Searcher searcher = indexShard.acquireSearcher("term_vector");
        try {
            Fields topLevelFields = MultiFields.getFields(get.searcher() != null ? get.searcher().reader() : searcher.reader());
            Versions.DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
            /* from an artificial document */
            if (request.doc() != null) {
                termVectorsByField = generateTermVectorsFromDoc(request, !docFromTranslog);
                // if no document indexed in shard, take the queried document itself for stats
                if (topLevelFields == null) {
                    topLevelFields = termVectorsByField;
                }
                termVectorsResponse.setArtificial(!docFromTranslog);
                termVectorsResponse.setExists(true);
            }
            /* or from an existing document */
            else if (docIdAndVersion != null) {
                // fields with stored term vectors
                termVectorsByField = docIdAndVersion.context.reader().getTermVectors(docIdAndVersion.docId);
                Set<String> selectedFields = request.selectedFields();
                // generate tvs for fields where analyzer is overridden
                if (selectedFields == null && request.perFieldAnalyzer() != null) {
                    selectedFields = getFieldsToGenerate(request.perFieldAnalyzer(), termVectorsByField);
                }
                // fields without term vectors
                if (selectedFields != null) {
                    termVectorsByField = addGeneratedTermVectors(get, termVectorsByField, request, selectedFields);
                }
                termVectorsResponse.setDocVersion(docIdAndVersion.version);
                termVectorsResponse.setExists(true);
            }
            /* no term vectors generated or found */
            else {
                termVectorsResponse.setExists(false);
            }
            /* if there are term vectors, optional compute dfs and/or terms filtering */
            if (termVectorsByField != null) {
                if (useDfs(request)) {
                    dfs = getAggregatedDfs(termVectorsByField, request);
                }

                if (request.filterSettings() != null) {
                    termVectorsFilter = new TermVectorsFilter(termVectorsByField, topLevelFields, request.selectedFields(), dfs);
                    termVectorsFilter.setSettings(request.filterSettings());
                    try {
                        termVectorsFilter.selectBestTerms();
                    } catch (IOException e) {
                        throw new ElasticsearchException("failed to select best terms", e);
                    }
                }
                // write term vectors
                termVectorsResponse.setFields(termVectorsByField, request.selectedFields(), request.getFlags(), topLevelFields, dfs, termVectorsFilter);
            }
        } catch (Throwable ex) {
            throw new ElasticsearchException("failed to execute term vector request", ex);
        } finally {
            searcher.close();
            get.release();
        }
        return termVectorsResponse;
    }

    private void handleFieldWildcards(TermVectorsRequest request) {
        Set<String> fieldNames = new HashSet<>();
        for (String pattern : request.selectedFields()) {
            fieldNames.addAll(indexShard.mapperService().simpleMatchToIndexNames(pattern));
        }
        request.selectedFields(fieldNames.toArray(Strings.EMPTY_ARRAY));
    }

    private boolean isValidField(FieldMapper field) {
        // must be a string
        if (!(field instanceof StringFieldMapper)) {
            return false;
        }
        // and must be indexed
        if (field.fieldType().indexOptions() == IndexOptions.NONE) {
            return false;
        }
        return true;
    }

    private Fields addGeneratedTermVectors(Engine.GetResult get, Fields termVectorsByField, TermVectorsRequest request, Set<String> selectedFields) throws IOException {
        /* only keep valid fields */
        Set<String> validFields = new HashSet<>();
        for (String field : selectedFields) {
            FieldMapper fieldMapper = indexShard.mapperService().smartNameFieldMapper(field);
            if (!isValidField(fieldMapper)) {
                continue;
            }
            // already retrieved, only if the analyzer hasn't been overridden at the field
            if (fieldMapper.fieldType().storeTermVectors() &&
                    (request.perFieldAnalyzer() == null || !request.perFieldAnalyzer().containsKey(field))) {
                continue;
            }
            validFields.add(field);
        }

        if (validFields.isEmpty()) {
            return termVectorsByField;
        }

        /* generate term vectors from fetched document fields */
        GetResult getResult = indexShard.getService().get(
                get, request.id(), request.type(), validFields.toArray(Strings.EMPTY_ARRAY), null, false);
        Fields generatedTermVectors = generateTermVectors(getResult.getFields().values(), request.offsets(), request.perFieldAnalyzer());

        /* merge with existing Fields */
        if (termVectorsByField == null) {
            return generatedTermVectors;
        } else {
            return mergeFields(termVectorsByField, generatedTermVectors);
        }
    }

    private Analyzer getAnalyzerAtField(String field, @Nullable Map<String, String> perFieldAnalyzer) {
        MapperService mapperService = indexShard.mapperService();
        Analyzer analyzer;
        if (perFieldAnalyzer != null && perFieldAnalyzer.containsKey(field)) {
            analyzer = mapperService.analysisService().analyzer(perFieldAnalyzer.get(field).toString());
        } else {
            analyzer = mapperService.smartNameFieldMapper(field).indexAnalyzer();
        }
        if (analyzer == null) {
            analyzer = mapperService.analysisService().defaultIndexAnalyzer();
        }
        return analyzer;
    }

    private Set<String> getFieldsToGenerate(Map<String, String> perAnalyzerField, Fields fieldsObject) {
        Set<String> selectedFields = new HashSet<>();
        for (String fieldName : fieldsObject) {
            if (perAnalyzerField.containsKey(fieldName)) {
                selectedFields.add(fieldName);
            }
        }
        return selectedFields;
    }

    private Fields generateTermVectors(Collection<GetField> getFields, boolean withOffsets, @Nullable Map<String, String> perFieldAnalyzer)
            throws IOException {
        /* store document in memory index */
        MemoryIndex index = new MemoryIndex(withOffsets);
        for (GetField getField : getFields) {
            String field = getField.getName();
            Analyzer analyzer = getAnalyzerAtField(field, perFieldAnalyzer);
            for (Object text : getField.getValues()) {
                index.addField(field, text.toString(), analyzer);
            }
        }
        /* and read vectors from it */
        return MultiFields.getFields(index.createSearcher().getIndexReader());
    }

    private Fields generateTermVectorsFromDoc(TermVectorsRequest request, boolean doAllFields) throws Throwable {
        // parse the document, at the moment we do update the mapping, just like percolate
        ParsedDocument parsedDocument = parseDocument(indexShard.shardId().getIndex(), request.type(), request.doc());

        // select the right fields and generate term vectors
        ParseContext.Document doc = parsedDocument.rootDoc();
        Collection<String> seenFields = new HashSet<>();
        Collection<GetField> getFields = new HashSet<>();
        for (IndexableField field : doc.getFields()) {
            FieldMapper fieldMapper = indexShard.mapperService().smartNameFieldMapper(field.name());
            if (seenFields.contains(field.name())) {
                continue;
            }
            else {
                seenFields.add(field.name());
            }
            if (!isValidField(fieldMapper)) {
                continue;
            }
            if (request.selectedFields() == null && !doAllFields && !fieldMapper.fieldType().storeTermVectors()) {
                continue;
            }
            if (request.selectedFields() != null && !request.selectedFields().contains(field.name())) {
                continue;
            }
            String[] values = doc.getValues(field.name());
            getFields.add(new GetField(field.name(), Arrays.asList((Object[]) values)));
        }
        return generateTermVectors(getFields, request.offsets(), request.perFieldAnalyzer());
    }

    private ParsedDocument parseDocument(String index, String type, BytesReference doc) throws Throwable {
        MapperService mapperService = indexShard.mapperService();

        // TODO: make parsing not dynamically create fields not in the original mapping
        Tuple<DocumentMapper, Mapping> docMapper = mapperService.documentMapperWithAutoCreate(type);
        ParsedDocument parsedDocument = docMapper.v1().parse(source(doc).type(type).flyweight(true));
        if (docMapper.v2() != null) {
            parsedDocument.addDynamicMappingsUpdate(docMapper.v2());
        }
        if (parsedDocument.dynamicMappingsUpdate() != null) {
            mappingUpdatedAction.updateMappingOnMasterSynchronously(index, type, parsedDocument.dynamicMappingsUpdate());
        }
        return parsedDocument;
    }

    private Fields mergeFields(Fields fields1, Fields fields2) throws IOException {
        ParallelFields parallelFields = new ParallelFields();
        for (String fieldName : fields2) {
            Terms terms = fields2.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        for (String fieldName : fields1) {
            if (parallelFields.fields.containsKey(fieldName)) {
                continue;
            }
            Terms terms = fields1.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        return parallelFields;
    }

    // Poached from Lucene ParallelLeafReader
    private static final class ParallelFields extends Fields {
        final Map<String,Terms> fields = new TreeMap<>();

        ParallelFields() {
        }

        void addField(String fieldName, Terms terms) {
            fields.put(fieldName, terms);
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) {
            return fields.get(field);
        }

        @Override
        public int size() {
            return fields.size();
        }
    }

    private boolean useDfs(TermVectorsRequest request) {
        return request.dfs() && (request.fieldStatistics() || request.termStatistics());
    }

    private AggregatedDfs getAggregatedDfs(Fields termVectorsFields, TermVectorsRequest request) throws IOException {
        DfsOnlyRequest dfsOnlyRequest = new DfsOnlyRequest(termVectorsFields, new String[]{request.index()},
                new String[]{request.type()}, request.selectedFields());
        DfsOnlyResponse response = dfsAction.execute(dfsOnlyRequest).actionGet();
        return response.getDfs();
    }
}
