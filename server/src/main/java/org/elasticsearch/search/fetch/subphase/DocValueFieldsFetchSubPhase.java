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
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * Query sub phase which pulls data from doc values
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class DocValueFieldsFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {

        if (context.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = context.collapse().getFieldType().name();
            if (context.docValueFieldsContext() == null) {
                context.docValueFieldsContext(new DocValueFieldsContext(Collections.singletonList(name)));
            } else if (context.docValueFieldsContext().fields().contains(name) == false) {
                context.docValueFieldsContext().fields().add(name);
            }
        }

        if (context.docValueFieldsContext() == null) {
            return;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, (a, b) -> Integer.compare(a.docId(), b.docId()));

        for (String field : context.docValueFieldsContext().fields()) {
            MappedFieldType fieldType = context.mapperService().fullName(field);
            if (fieldType != null) {
                LeafReaderContext subReaderContext = null;
                AtomicFieldData data = null;
                ScriptDocValues<?> values = null;
                for (SearchHit hit : hits) {
                    // if the reader index has changed we need to get a new doc values reader instance
                    if (subReaderContext == null || hit.docId() >= subReaderContext.docBase + subReaderContext.reader().maxDoc()) {
                        int readerIndex = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
                        subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                        data = context.getForField(fieldType).load(subReaderContext);
                        values = data.getScriptValues();
                    }
                    int subDocId = hit.docId() - subReaderContext.docBase;
                    values.setNextDocId(subDocId);
                    if (hit.fieldsOrNull() == null) {
                        hit.fields(new HashMap<>(2));
                    }
                    DocumentField hitField = hit.getFields().get(field);
                    if (hitField == null) {
                        hitField = new DocumentField(field, new ArrayList<>(2));
                        hit.getFields().put(field, hitField);
                    }
                    hitField.getValues().addAll(values);
                }
            }
        }
    }
}
