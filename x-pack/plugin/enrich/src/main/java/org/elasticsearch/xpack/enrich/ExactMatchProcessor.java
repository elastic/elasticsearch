/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class ExactMatchProcessor extends AbstractProcessor {

    private final Function<String, EnrichPolicy> policyLookup;
    private final Function<String, Engine.Searcher> searchProvider;

    private final String policyName;
    private final String key;
    private final boolean ignoreKeyMissing;
    private final List<EnrichProcessorFactory.EnrichSpecification> specifications;

    ExactMatchProcessor(String tag,
                        Function<String, EnrichPolicy> policyLookup,
                        Function<String, Engine.Searcher> searchProvider, String policyName,
                        String key,
                        boolean ignoreKeyMissing,
                        List<EnrichProcessorFactory.EnrichSpecification> specifications) {
        super(tag);
        this.policyLookup = policyLookup;
        this.searchProvider = searchProvider;
        this.policyName = policyName;
        this.key = key;
        this.ignoreKeyMissing = ignoreKeyMissing;
        this.specifications = specifications;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        final EnrichPolicy policy = policyLookup.apply(policyName);
        if (policy == null) {
            throw new IllegalArgumentException("policy [" + policyName + "] does not exists");
        }

        final String value = ingestDocument.getFieldValue(key, String.class, ignoreKeyMissing);
        if (value == null) {
            return ingestDocument;
        }

        // TODO: re-use the engine searcher between enriching documents from the same write request
        try (Engine.Searcher engineSearcher = searchProvider.apply(policy.getIndexPattern())) {
            if (engineSearcher.getDirectoryReader().leaves().size() != 1) {
                throw new IllegalStateException("enrich index must have exactly a single segment");
            }

            final LeafReader leafReader = engineSearcher.getDirectoryReader().leaves().get(0).reader();
            final Terms terms = leafReader.terms(policy.getEnrichKey());
            if (terms == null) {
                return ingestDocument;
            }

            final TermsEnum tenum = terms.iterator();
            if (tenum.seekExact(new BytesRef(value))) {
                PostingsEnum penum = tenum.postings(null, PostingsEnum.NONE);
                final int docId = penum.nextDoc();
                assert docId != PostingsEnum.NO_MORE_DOCS : "no matching doc id for [" + key + "]";
                assert penum.nextDoc() == PostingsEnum.NO_MORE_DOCS : "more than one doc id matching for [" + key + "]";

                BinaryDocValues enrichSourceField = leafReader.getBinaryDocValues(EnrichSourceFieldMapper.NAME);
                assert enrichSourceField != null : "enrich source field is missing";

                boolean exact = enrichSourceField.advanceExact(docId);
                assert exact : "doc id [" + docId + "] doesn't have binary doc values";

                final BytesReference encoded = new BytesArray(enrichSourceField.binaryValue());
                final Map<String, Object> decoded =
                    XContentHelper.convertToMap(encoded, false, XContentType.SMILE).v2();
                for (EnrichProcessorFactory.EnrichSpecification specification : specifications) {
                    Object enrichValue = decoded.get(specification.sourceField);
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

    String getKey() {
        return key;
    }

    boolean isIgnoreKeyMissing() {
        return ignoreKeyMissing;
    }

    List<EnrichProcessorFactory.EnrichSpecification> getSpecifications() {
        return specifications;
    }
}
