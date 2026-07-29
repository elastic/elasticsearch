/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.get.ShardGetService.shouldExcludeInferenceFieldsFromSource;

public final class FetchSourcePhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchSourceContext fetchSourceContext = fetchContext.fetchSourceContext();
        if (fetchSourceContext == null || fetchSourceContext.fetchSource() == false) {
            return null;
        }
        assert fetchSourceContext.fetchSource();
        SourceFilter sourceFilter = fetchSourceContext.filter();
        final boolean filterExcludesAll = sourceFilter != null && sourceFilter.excludesAll();
        final ValueFetcher inferenceFieldsValueFetcher = getInferenceFieldsValueFetcher(fetchContext);

        return new FetchSubPhaseProcessor() {
            private int fastPath;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                if (inferenceFieldsValueFetcher != null) {
                    inferenceFieldsValueFetcher.setNextReader(readerContext);
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                String index = fetchContext.getIndexName();
                if (fetchContext.getSearchExecutionContext().isSourceEnabled() == false) {
                    if (sourceFilter != null) {
                        throw new IllegalArgumentException(
                            "unable to fetch fields from _source field: _source is disabled in the mappings for index [" + index + "]"
                        );
                    }
                    return;
                }
                hitExecute(hitContext);
            }

            private void hitExecute(HitContext hitContext) throws IOException {
                final boolean nestedHit = hitContext.hit().getNestedIdentity() != null;
                Source source = hitContext.source();

                // If this is a parent document and there are no source filters, then add the source as-is.
                if (nestedHit == false && sourceFilter == null) {
                    source = addInferenceMetadataFields(hitContext.hit(), hitContext.docId(), source);
                    hitContext.hit().sourceRef(source.internalSourceRef());
                    fastPath++;
                    return;
                }

                if (filterExcludesAll) {
                    // we can just add an empty map
                    source = Source.empty(source.sourceContentType());
                } else {
                    // Otherwise, filter the source and add it to the hit.
                    source = sourceFilter != null ? source.filter(sourceFilter) : source;
                }
                if (nestedHit) {
                    source = extractNested(source, hitContext.hit().getNestedIdentity());
                } else {
                    source = addInferenceMetadataFields(hitContext.hit(), hitContext.docId(), source);
                }
                hitContext.hit().sourceRef(source.internalSourceRef());
            }

            /**
             * Adds the {@link InferenceMetadataFieldsMapper#NAME} field to the {@code _source} when it has been
             * requested.
             *
             * <p>If {@link org.elasticsearch.search.fetch.subphase.FetchFieldsPhase} already produced a
             * {@link DocumentField} for {@code _inference_fields} (because the user also requested it via the fields
             * API), that value is reused without touching the document-field entry so the fields-API output is
             * preserved. Otherwise the value fetcher is invoked directly, mirroring the GET path in
             * {@link org.elasticsearch.index.get.ShardGetService}.
             */
            private Source addInferenceMetadataFields(SearchHit hit, int docId, Source source) throws IOException {
                if (inferenceFieldsValueFetcher == null) {
                    return source;
                }

                // Peek the value FetchFieldsPhase already produced when the user requested it via the
                // fields API; do NOT remove it so the fields-API output remains intact.
                DocumentField existing = hit.field(InferenceMetadataFieldsMapper.NAME);
                final Object value;
                if (existing != null && existing.getValues().isEmpty() == false) {
                    assert existing.getValues().size() == 1;
                    value = existing.getValues().getFirst();
                } else {
                    List<Object> values = inferenceFieldsValueFetcher.fetchValues(source, docId, List.of());
                    if (values.isEmpty()) {
                        return source;
                    }

                    assert values.size() == 1;
                    value = values.getFirst();
                }

                return source.withMutations(map -> map.put(InferenceMetadataFieldsMapper.NAME, value));
            }

            @Override
            public Map<String, Object> getDebugInfo() {
                return Map.of("fast_path", fastPath);
            }
        };
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

    private static ValueFetcher getInferenceFieldsValueFetcher(FetchContext fetchContext) {
        FetchSourceContext fetchSourceContext = fetchContext.fetchSourceContext();
        SearchExecutionContext searchExecutionContext = fetchContext.getSearchExecutionContext();
        MappingLookup mappingLookup = searchExecutionContext.getMappingLookup();

        ValueFetcher valueFetcher = null;
        if (InferenceMetadataFieldsMapper.isEnabled(mappingLookup)
            && mappingLookup.inferenceFields().isEmpty() == false
            && shouldExcludeInferenceFieldsFromSource(fetchSourceContext) == false) {
            var inferenceMetadataFieldsMapper = mappingLookup.getMapping().getMetadataMapperByName(InferenceMetadataFieldsMapper.NAME);
            valueFetcher = inferenceMetadataFieldsMapper.fieldType().valueFetcher(searchExecutionContext, null);
        }

        return valueFetcher;
    }
}
