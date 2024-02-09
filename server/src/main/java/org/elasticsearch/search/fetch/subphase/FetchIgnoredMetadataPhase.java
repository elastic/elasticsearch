/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FetchIgnoredMetadataPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        StoredFieldsContext storedFieldsContext = fetchContext.storedFieldsContext();
        if (storedFieldsContext == null || storedFieldsContext.fetchFields() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            SortedSetDocValues ignoredFields = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                ignoredFields = readerContext.reader().getSortedSetDocValues(IgnoredFieldMapper.NAME);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                if (ignoredFields != null && ignoredFields.advanceExact(hitContext.docId())) {
                    final List<Object> values = new ArrayList<>();
                    long ordIndex = ignoredFields.nextOrd();
                    while (ordIndex != SortedSetDocValues.NO_MORE_ORDS) {
                        values.add(ignoredFields.lookupOrd(ordIndex).utf8ToString());
                        ordIndex = ignoredFields.nextOrd();
                    }
                    final DocumentField ignoredField = new DocumentField(
                        IgnoredFieldMapper.NAME,
                        values,
                        Collections.emptyList(),
                        Collections.emptyList()
                    );
                    hitContext.hit().addDocumentFields(Collections.emptyMap(), Map.of(IgnoredFieldMapper.NAME, ignoredField));
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }
}
