/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class ExactMatchProcessor extends AbstractProcessor {

    static final String ENRICH_KEY_FIELD_NAME = "enrich_key_field";

    private final Function<String, Tuple<IndexMetaData, Engine.Searcher>> searchProvider;

    private final String policyName;
    private final String enrichKey;
    private final boolean ignoreMissing;
    private final List<EnrichSpecification> specifications;

    ExactMatchProcessor(String tag,
                        Function<String, Tuple<IndexMetaData, Engine.Searcher>> searchProvider,
                        String policyName,
                        String enrichKey,
                        boolean ignoreMissing,
                        List<EnrichSpecification> specifications) {
        super(tag);
        this.searchProvider = searchProvider;
        this.policyName = policyName;
        this.enrichKey = enrichKey;
        this.ignoreMissing = ignoreMissing;
        this.specifications = specifications;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final String value = ingestDocument.getFieldValue(enrichKey, String.class, ignoreMissing);
        if (value == null) {
            return ingestDocument;
        }

        // TODO: re-use the engine searcher between enriching documents from the same write request
        Tuple<IndexMetaData, Engine.Searcher> tuple = searchProvider.apply(EnrichPolicy.getBaseName(policyName));
        String enrichKeyField = getEnrichKeyField(tuple.v1());

        try (Engine.Searcher engineSearcher = tuple.v2()) {
            if (engineSearcher.getDirectoryReader().leaves().size() == 0) {
                return ingestDocument;
            } else if (engineSearcher.getDirectoryReader().leaves().size() != 1) {
                throw new IllegalStateException("enrich index must have exactly a single segment");
            }

            final LeafReader leafReader = engineSearcher.getDirectoryReader().leaves().get(0).reader();
            final Terms terms = leafReader.terms(enrichKeyField);
            if (terms == null) {
                throw new IllegalStateException("enrich key field does not exist");
            }

            final TermsEnum tenum = terms.iterator();
            if (tenum.seekExact(new BytesRef(value))) {
                PostingsEnum penum = tenum.postings(null, PostingsEnum.NONE);
                final int docId = penum.nextDoc();
                assert docId != PostingsEnum.NO_MORE_DOCS : "no matching doc id for [" + enrichKey + "]";
                assert penum.nextDoc() == PostingsEnum.NO_MORE_DOCS : "more than one doc id matching for [" + enrichKey + "]";

                // TODO: The use of _source is temporarily until enrich source field mapper has been added (see PR #41521)
                Document document = leafReader.document(docId, Collections.singleton(SourceFieldMapper.NAME));
                BytesRef source = document.getBinaryValue(SourceFieldMapper.NAME);
                assert source != null;

                final BytesReference encoded = new BytesArray(source);
                final Map<String, Object> decoded =
                    XContentHelper.convertToMap(encoded, false, XContentType.SMILE).v2();
                for (EnrichSpecification specification : specifications) {
                    Object enrichValue = decoded.get(specification.sourceField);
                    // TODO: add support over overwrite option (like in SetProcessor)
                    ingestDocument.setFieldValue(specification.targetField, enrichValue);
                }
            }
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return EnrichProcessorFactory.TYPE;
    }

    String getPolicyName() {
        return policyName;
    }

    String getEnrichKey() {
        return enrichKey;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    List<EnrichSpecification> getSpecifications() {
        return specifications;
    }

    private static String getEnrichKeyField(IndexMetaData imd) {
        if (imd == null) {
            throw new IllegalStateException("enrich index is missing");
        }

        Map<String, Object> mappingSource = imd.mapping().getSourceAsMap();
        Map<?, ?> meta = (Map<?, ?>) mappingSource.get("_meta");
        if (meta == null) {
            throw new IllegalStateException("_meta field is missing in enrich index");
        }

        String fieldName = (String) meta.get(ENRICH_KEY_FIELD_NAME);
        if (fieldName == null) {
            throw new IllegalStateException("enrich key fieldname missing");
        }
        return fieldName;
    }
}
