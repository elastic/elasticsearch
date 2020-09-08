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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fetch sub phase which pulls data from doc values.
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class FetchDocValuesPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(SearchContext context, SearchLookup lookup) throws IOException {
        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldName();
            if (context.docValuesContext() == null) {
                context.docValuesContext(new FetchDocValuesContext(
                    Collections.singletonList(new FieldAndFormat(name, null))));
            } else if (context.docValuesContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                context.docValuesContext().fields().add(new FieldAndFormat(name, null));
            }
        }

        if (context.docValuesContext() == null) {
            return null;
        }

        Map<String, ValueFetcher> fields = new LinkedHashMap<>(context.docValuesContext().fields().size());
        for (FieldAndFormat fieldAndFormat : context.docValuesContext().fields()) {
            MappedFieldType ft = context.mapperService().fieldType(fieldAndFormat.field);
            if (ft == null) {
                continue;
            }
            ValueFetcher fetcher = new DocValueFetcher(ft.docValueFormat(fieldAndFormat.format, null), lookup.doc().getForField(ft));
            fields.put(fieldAndFormat.field, fetcher);
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                for (ValueFetcher f : fields.values()) {
                    f.setNextReader(readerContext);
                }
            }

            @Override
            public void process(HitContext hit) throws IOException {
                for (Map.Entry<String, ValueFetcher> f : fields.entrySet()) {
                    DocumentField hitField = hit.hit().field(f.getKey());
                    if (hitField == null) {
                        hitField = new DocumentField(f.getKey(), new ArrayList<>(2));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.hit().setDocumentField(f.getKey(), hitField);
                    }
                    hitField.getValues().addAll(f.getValue().fetchValues(hit.sourceLookup()));
                }
            }
        };
    }
}
