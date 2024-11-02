/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FieldFetcher;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sub phase within the fetch phase used to fetch things *about* the documents like highlighting or matched queries.
 */
@FunctionalInterface
public interface FetchSubPhase {

    class HitContext {
        private final SearchHit hit;
        private final LeafReaderContext readerContext;
        private final int docId;
        private final Source source;
        private final Map<String, List<Object>> loadedFields;
        private final RankDoc rankDoc;

        public HitContext(
            SearchHit hit,
            LeafReaderContext context,
            int docId,
            Map<String, List<Object>> loadedFields,
            Source source,
            RankDoc rankDoc
        ) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.source = source;
            this.loadedFields = loadedFields;
            this.rankDoc = rankDoc;
        }

        public SearchHit hit() {
            return hit;
        }

        public LeafReader reader() {
            return readerContext.reader();
        }

        public LeafReaderContext readerContext() {
            return readerContext;
        }

        /**
         * @return the docId of this hit relative to the leaf reader context
         */
        public int docId() {
            return docId;
        }

        /**
         * This lookup provides access to the source for the given hit document. Note
         * that it should always be set to the correct doc ID and {@link LeafReaderContext}.
         *
         * In most cases, the hit document's source is loaded eagerly at the start of the
         * {@link FetchPhase}. This lookup will contain the preloaded source.
         */
        public Source source() {
            return source;
        }

        public Map<String, List<Object>> loadedFields() {
            return loadedFields;
        }

        @Nullable
        public RankDoc rankDoc() {
            return this.rankDoc;
        }

        public IndexReader topLevelReader() {
            return ReaderUtil.getTopLevelContext(readerContext).reader();
        }
    }

    /**
     * Returns a {@link FetchSubPhaseProcessor} for this sub phase.
     *
     * If nothing should be executed for the provided {@code FetchContext}, then the
     * implementation should return {@code null}
     */
    FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException;

    /**
     * Explains the scoring calculations for the top hits.
     */
    FetchSubPhase EXPLAIN = context -> {
        if (context.explain() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            private final List<String> queryNames = context.queryNames();

            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public void process(HitContext hitContext) throws IOException {
                final int topLevelDocId = hitContext.hit().docId();
                Explanation explanation = context.searcher().explain(context.rewrittenQuery(), topLevelDocId);

                for (RescoreContext rescore : context.rescore()) {
                    explanation = rescore.rescorer().explain(topLevelDocId, context.searcher(), rescore, explanation);
                }

                if (context.rankBuilder() != null) {
                    // if we have nested fields, then the query is wrapped using an additional filter on the _primary_term field
                    // through the DefaultSearchContext#buildFilteredQuery so we have to extract the actual query
                    if (context.getSearchExecutionContext().nestedLookup() != NestedLookup.EMPTY) {
                        explanation = explanation.getDetails()[0];
                    }

                    if (context.rankBuilder() != null) {
                        explanation = context.rankBuilder().explainHit(explanation, hitContext.rankDoc(), queryNames);
                    }
                }
                // we use the top level doc id, since we work with the top level searcher
                hitContext.hit().explanation(explanation);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    };

    /**
     * Process stored fields loaded from a HitContext into DocumentFields
     */
    FetchSubPhase STORED_FIELDS = context -> {
        /** Associates a field name with a mapped field type and whether or not it is a metadata field */
        record StoredField(String name, MappedFieldType ft) {
            /** Processes a set of stored fields using field type information */
            List<Object> process(Map<String, List<Object>> loadedFields) {
                List<Object> inputs = loadedFields.get(ft.name());
                if (inputs == null) {
                    return List.of();
                }
                // This is eventually provided to DocumentField, which needs this collection to be mutable
                return inputs.stream().map(ft::valueForDisplay).collect(Collectors.toList());
            }

            boolean hasValue(Map<String, List<Object>> loadedFields) {
                return loadedFields.containsKey(ft.name());
            }
        }

        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();
        if (storedFieldsContext == null || storedFieldsContext.fetchFields() == false) {
            return null;
        }

        // build the StoredFieldsSpec and a list of StoredField records to process
        List<StoredField> storedFields = new ArrayList<>();
        Set<String> fieldsToLoad = new HashSet<>();
        if (storedFieldsContext.fieldNames() != null) {
            SearchExecutionContext sec = context.getSearchExecutionContext();
            for (String field : storedFieldsContext.fieldNames()) {
                Collection<String> fieldNames = sec.getMatchingFieldNames(field);
                for (String fieldName : fieldNames) {
                    // _id and _source are always retrieved anyway, no need to do it explicitly. See FieldsVisitor.
                    // They are not returned as part of HitContext#loadedFields hence they are not added to documents by this sub-phase
                    if (IdFieldMapper.NAME.equals(field) || SourceFieldMapper.NAME.equals(field)) {
                        continue;
                    }
                    MappedFieldType ft = sec.getFieldType(fieldName);
                    if (ft.isStored() == false || sec.isMetadataField(fieldName)) {
                        continue;
                    }
                    storedFields.add(new StoredField(fieldName, ft));
                    fieldsToLoad.add(ft.name());
                }
            }
        }
        StoredFieldsSpec storedFieldsSpec = new StoredFieldsSpec(false, true, fieldsToLoad);

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public void process(HitContext hitContext) {
                Map<String, List<Object>> loadedFields = hitContext.loadedFields();
                for (StoredField storedField : storedFields) {
                    if (storedField.hasValue(loadedFields)) {
                        hitContext.hit()
                            .setDocumentField(storedField.name, new DocumentField(storedField.name, storedField.process(loadedFields)));
                    }
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return storedFieldsSpec;
            }
        };
    };

    /**
     * Fetch sub phase which pulls data from doc values.
     *
     * Specifying {@code "docvalue_fields": ["field1", "field2"]}
     */
    FetchSubPhase FETCH_DOC_VALUES = context -> {
        record DocValueField(String field, ValueFetcher fetcher) {}

        FetchDocValuesContext dvContext = context.docValuesContext();
        if (dvContext == null) {
            return null;
        }

        /*
         * Its tempting to swap this to a `Map` but that'd break backwards
         * compatibility because we support fetching the same field multiple
         * times with different configuration. That isn't possible with a `Map`.
         */
        List<DocValueField> fields = new ArrayList<>();
        for (FieldAndFormat fieldAndFormat : dvContext.fields()) {
            SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
            MappedFieldType ft = searchExecutionContext.getFieldType(fieldAndFormat.field);
            if (ft == null) {
                continue;
            }
            ValueFetcher fetcher = new DocValueFetcher(
                ft.docValueFormat(fieldAndFormat.format, null),
                searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH)
            );
            fields.add(new DocValueField(fieldAndFormat.field, fetcher));
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                for (DocValueField f : fields) {
                    f.fetcher.setNextReader(readerContext);
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hit) throws IOException {
                for (DocValueField f : fields) {
                    DocumentField hitField = hit.hit().field(f.field);
                    if (hitField == null) {
                        hitField = new DocumentField(f.field, new ArrayList<>(2));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.hit().setDocumentField(f.field, hitField);
                    }
                    List<Object> ignoredValues = new ArrayList<>();
                    hitField.getValues().addAll(f.fetcher.fetchValues(hit.source(), hit.docId(), ignoredValues));
                    // Doc value fetches should not return any ignored values
                    assert ignoredValues.isEmpty();
                }
            }
        };
    };

    FetchSubPhase SCRIPT_FIELDS = context -> {
        if (context.scriptFields() == null || context.scriptFields().fields().isEmpty()) {
            return null;
        }
        List<ScriptFieldsContext.ScriptField> scriptFields = context.scriptFields().fields();
        return new FetchSubPhaseProcessor() {

            FieldScript[] leafScripts = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                FieldScript[] scripts = new FieldScript[scriptFields.size()];
                for (int i = 0; i < scripts.length; i++) {
                    try {
                        scripts[i] = scriptFields.get(i).script().newInstance(readerContext);
                    } catch (IOException e1) {
                        throw new IllegalStateException("Failed to load script " + scriptFields.get(i).name(), e1);
                    }
                }
                leafScripts = scripts;
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                // If script fields need source then they will load it via SearchLookup,
                // which has its own lazy loading config that kicks in if not overridden
                // by other sub phases that require source. However, if script fields
                // are present then we enforce metadata loading
                return new StoredFieldsSpec(false, true, Set.of());
            }

            @Override
            public void process(HitContext hitContext) {
                int docId = hitContext.docId();
                for (int i = 0; i < leafScripts.length; i++) {
                    leafScripts[i].setDocument(docId);
                    final Object value;
                    try {
                        value = leafScripts[i].execute();
                        CollectionUtils.ensureNoSelfReferences(value, "ScriptFieldsPhase leaf script " + i);
                    } catch (RuntimeException e) {
                        if (scriptFields.get(i).ignoreException()) {
                            continue;
                        }
                        throw e;
                    }
                    String scriptFieldName = scriptFields.get(i).name();
                    DocumentField hitField = hitContext.hit().field(scriptFieldName);
                    if (hitField == null) {
                        final List<Object> values;
                        if (value instanceof Collection) {
                            values = new ArrayList<>((Collection<?>) value);
                        } else {
                            values = Collections.singletonList(value);
                        }
                        hitField = new DocumentField(scriptFieldName, values);
                        // script fields are never meta-fields
                        hitContext.hit().setDocumentField(scriptFieldName, hitField);
                    }
                }
            }
        };
    };

    FetchSubPhase FETCH_SOURCE = fetchContext -> {
        FetchSourceContext fetchSourceContext = fetchContext.fetchSourceContext();
        if (fetchSourceContext == null || fetchSourceContext.fetchSource() == false) {
            return null;
        }
        assert fetchSourceContext.fetchSource();
        SourceFilter sourceFilter = fetchSourceContext.filter();
        final boolean filterExcludesAll = sourceFilter.excludesAll();
        return new FetchSubPhaseProcessor() {
            private int fastPath;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }

            @Override
            public void process(HitContext hitContext) {
                String index = fetchContext.getIndexName();
                if (fetchContext.getSearchExecutionContext().isSourceEnabled() == false) {
                    if (fetchSourceContext.hasFilter()) {
                        throw new IllegalArgumentException(
                            "unable to fetch fields from _source field: _source is disabled in the mappings for index [" + index + "]"
                        );
                    }
                    return;
                }
                hitExecute(fetchSourceContext, hitContext);
            }

            private void hitExecute(FetchSourceContext fetchSourceContext, HitContext hitContext) {
                final boolean nestedHit = hitContext.hit().getNestedIdentity() != null;
                Source source = hitContext.source();

                // If this is a parent document and there are no source filters, then add the source as-is.
                if (nestedHit == false && fetchSourceContext.hasFilter() == false) {
                    hitContext.hit().sourceRef(source.internalSourceRef());
                    fastPath++;
                    return;
                }

                if (filterExcludesAll) {
                    // we can just add an empty map
                    source = Source.empty(source.sourceContentType());
                } else {
                    // Otherwise, filter the source and add it to the hit.
                    source = source.filter(sourceFilter);
                }
                if (nestedHit) {
                    source = extractNested(source, hitContext.hit().getNestedIdentity());
                }
                hitContext.hit().sourceRef(source.internalSourceRef());
            }

            @Override
            public Map<String, Object> getDebugInfo() {
                return Map.of("fast_path", fastPath);
            }

            @SuppressWarnings("unchecked")
            private static Source extractNested(Source in, SearchHit.NestedIdentity nestedIdentity) {
                Map<String, Object> sourceMap = in.source();
                while (nestedIdentity != null) {
                    sourceMap = (Map<String, Object>) sourceMap.get(nestedIdentity.getField().string());
                    if (sourceMap == null) {
                        return Source.empty(in.sourceContentType());
                    }
                    nestedIdentity = nestedIdentity.getChild();
                }
                return Source.fromMap(sourceMap, in.sourceContentType());
            }
        };
    };

    /**
     * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
     * retrieves the field values through the relevant {@link org.elasticsearch.index.mapper.ValueFetcher}
     * and returns them as document fields.
     */
    FetchSubPhase FETCH_FIELDS = fetchContext -> {
        final FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        final StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();

        boolean fetchStoredFields = storedFieldsContext != null && storedFieldsContext.fetchFields();
        if (fetchFieldsContext == null && fetchStoredFields == false) {
            return null;
        }

        // NOTE: FieldFetcher for non-metadata fields, as well as `_id` and `_source`.
        // We need to retain `_id` and `_source` here to correctly populate the `StoredFieldSpecs` created by the
        // `FieldFetcher` constructor.
        final SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
        final FieldFetcher fieldFetcher = (fetchFieldsContext == null
            || fetchFieldsContext.fields() == null
            || fetchFieldsContext.fields().isEmpty())
                ? null
                : FieldFetcher.create(
                    searchExecutionContext,
                    fetchFieldsContext.fields()
                        .stream()
                        .filter(
                            fieldAndFormat -> (searchExecutionContext.isMetadataField(fieldAndFormat.field) == false
                                || searchExecutionContext.getFieldType(fieldAndFormat.field).isStored() == false
                                || IdFieldMapper.NAME.equals(fieldAndFormat.field)
                                || SourceFieldMapper.NAME.equals(fieldAndFormat.field))
                        )
                        .toList()
                );

        // NOTE: Collect stored metadata fields requested via `fields` (in FetchFieldsContext) like for instance the _ignored source field
        final Set<FieldAndFormat> fetchContextMetadataFields = new HashSet<>();
        if (fetchFieldsContext != null && fetchFieldsContext.fields() != null && fetchFieldsContext.fields().isEmpty() == false) {
            for (final FieldAndFormat fieldAndFormat : fetchFieldsContext.fields()) {
                // NOTE: _id and _source are always retrieved anyway, no need to do it explicitly. See FieldsVisitor.
                if (SourceFieldMapper.NAME.equals(fieldAndFormat.field) || IdFieldMapper.NAME.equals(fieldAndFormat.field)) {
                    continue;
                }
                if (searchExecutionContext.isMetadataField(fieldAndFormat.field)
                    && searchExecutionContext.getFieldType(fieldAndFormat.field).isStored()) {
                    fetchContextMetadataFields.add(fieldAndFormat);
                }
            }
        }

        final FieldFetcher metadataFieldFetcher;
        if (storedFieldsContext != null
            && storedFieldsContext.fieldNames() != null
            && storedFieldsContext.fieldNames().isEmpty() == false) {
            final Set<FieldAndFormat> metadataFields = new HashSet<>(FieldFetcher.DEFAULT_METADATA_FIELDS);
            for (final String storedField : storedFieldsContext.fieldNames()) {
                final Set<String> matchingFieldNames = searchExecutionContext.getMatchingFieldNames(storedField);
                for (final String matchingFieldName : matchingFieldNames) {
                    if (SourceFieldMapper.NAME.equals(matchingFieldName) || IdFieldMapper.NAME.equals(matchingFieldName)) {
                        continue;
                    }
                    final MappedFieldType fieldType = searchExecutionContext.getFieldType(matchingFieldName);
                    // NOTE: Exclude _ignored_source when requested via wildcard '*'
                    if (matchingFieldName.equals(IgnoredSourceFieldMapper.NAME) && Regex.isSimpleMatchPattern(storedField)) {
                        continue;
                    }
                    // NOTE: checking if the field is stored is required for backward compatibility reasons and to make
                    // sure we also handle here stored fields requested via `stored_fields`, which was previously a
                    // responsibility of StoredFieldsPhase.
                    if (searchExecutionContext.isMetadataField(matchingFieldName) && fieldType.isStored()) {
                        metadataFields.add(new FieldAndFormat(matchingFieldName, null));
                    }
                }
            }
            // NOTE: Include also metadata stored fields requested via `fields`
            metadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, metadataFields);
        } else {
            // NOTE: Include also metadata stored fields requested via `fields`
            final Set<FieldAndFormat> allMetadataFields = new HashSet<>(FieldFetcher.DEFAULT_METADATA_FIELDS);
            allMetadataFields.addAll(fetchContextMetadataFields);
            metadataFieldFetcher = FieldFetcher.create(searchExecutionContext, allMetadataFields);
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (fieldFetcher != null) {
                    fieldFetcher.setNextReader(readerContext);
                }
                metadataFieldFetcher.setNextReader(readerContext);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                if (fieldFetcher != null) {
                    return metadataFieldFetcher.storedFieldsSpec().merge(fieldFetcher.storedFieldsSpec());
                }
                return metadataFieldFetcher.storedFieldsSpec();
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final Map<String, DocumentField> fields = fieldFetcher != null
                    ? fieldFetcher.fetch(hitContext.source(), hitContext.docId())
                    : Collections.emptyMap();
                final Map<String, DocumentField> metadataFields = metadataFieldFetcher.fetch(hitContext.source(), hitContext.docId());
                hitContext.hit().addDocumentFields(fields, metadataFields);
            }
        };
    };

    FetchSubPhase FETCH_VERSION = context -> {
        if (context.version() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            NumericDocValues versions = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                versions = readerContext.reader().getNumericDocValues(VersionFieldMapper.NAME);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                long version = Versions.NOT_FOUND;
                if (versions != null && versions.advanceExact(hitContext.docId())) {
                    version = versions.longValue();
                }
                hitContext.hit().version(version < 0 ? -1 : version);
            }
        };
    };

    FetchSubPhase SEQ_NO_PRIMARY_TERM = context -> {
        if (context.seqNoAndPrimaryTerm() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            NumericDocValues seqNoField = null;
            NumericDocValues primaryTermField = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                seqNoField = readerContext.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                primaryTermField = readerContext.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                int docId = hitContext.docId();
                long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
                // we have to check the primary term field as it is only assigned for non-nested documents
                if (primaryTermField != null && primaryTermField.advanceExact(docId)) {
                    boolean found = seqNoField.advanceExact(docId);
                    assert found : "found seq no for " + docId + " but not a primary term";
                    seqNo = seqNoField.longValue();
                    primaryTerm = primaryTermField.longValue();
                }
                hitContext.hit().setSeqNo(seqNo);
                hitContext.hit().setPrimaryTerm(primaryTerm);
            }
        };
    };

    FetchSubPhase MATCHED_QUERIES = context -> {
        Map<String, Query> namedQueries = new HashMap<>();
        if (context.parsedQuery() != null) {
            namedQueries.putAll(context.parsedQuery().namedFilters());
        }
        if (context.parsedPostFilter() != null) {
            namedQueries.putAll(context.parsedPostFilter().namedFilters());
        }
        for (RescoreContext rescoreContext : context.rescore()) {
            for (ParsedQuery parsedQuery : rescoreContext.getParsedQueries()) {
                namedQueries.putAll(parsedQuery.namedFilters());
            }
        }
        if (namedQueries.isEmpty()) {
            return null;
        }
        Map<String, Weight> weights = new HashMap<>();
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            weights.put(
                entry.getKey(),
                context.searcher().createWeight(context.searcher().rewrite(entry.getValue()), ScoreMode.COMPLETE, 1)
            );
        }
        return new FetchSubPhaseProcessor() {

            final Map<String, Scorer> matchingIterators = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                matchingIterators.clear();
                for (Map.Entry<String, Weight> entry : weights.entrySet()) {
                    ScorerSupplier ss = entry.getValue().scorerSupplier(readerContext);
                    if (ss != null) {
                        Scorer scorer = ss.get(0L);
                        if (scorer != null) {
                            matchingIterators.put(entry.getKey(), scorer);
                        }
                    }
                }
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, Float> matches = new LinkedHashMap<>();
                int doc = hitContext.docId();
                for (Map.Entry<String, Scorer> entry : matchingIterators.entrySet()) {
                    Scorer scorer = entry.getValue();
                    if (scorer.iterator().docID() < doc) {
                        scorer.iterator().advance(doc);
                    }
                    if (scorer.iterator().docID() == doc) {
                        matches.put(entry.getKey(), scorer.score());
                    }
                }
                hitContext.hit().matchedQueries(matches);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    };

    FetchSubPhase FETCH_SCORE = context -> {
        if (context.fetchScores() == false) {
            return null;
        }
        final IndexSearcher searcher = context.searcher();
        final Weight weight = searcher.createWeight(context.rewrittenQuery(), ScoreMode.COMPLETE, 1);
        return new FetchSubPhaseProcessor() {

            Scorer scorer;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                ScorerSupplier scorerSupplier = weight.scorerSupplier(readerContext);
                if (scorerSupplier == null) {
                    throw new IllegalStateException("Can't compute score on document as it doesn't match the query");
                }
                scorer = scorerSupplier.get(1L); // random-access
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                if (scorer == null || scorer.iterator().advance(hitContext.docId()) != hitContext.docId()) {
                    throw new IllegalStateException("Can't compute score on document " + hitContext + " as it doesn't match the query");
                }
                hitContext.hit().score(scorer.score());
            }
        };
    };
}
