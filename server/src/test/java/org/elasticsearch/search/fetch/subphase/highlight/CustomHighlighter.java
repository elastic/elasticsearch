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
package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * total dumb highlighter used to test the pluggable highlighting functionality
 */
public class CustomHighlighter implements Highlighter {

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        SearchHighlightContext.Field field = fieldContext.field;
        CacheEntry cacheEntry = (CacheEntry) fieldContext.hitContext.cache().get("test-custom");
        final int docId = fieldContext.hitContext.readerContext().docBase + fieldContext.hitContext.docId();
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            fieldContext.hitContext.cache().put("test-custom", cacheEntry);
            cacheEntry.docId = docId;
            cacheEntry.position = 1;
        } else {
            if (cacheEntry.docId == docId) {
                cacheEntry.position++;
            } else {
                cacheEntry.docId = docId;
                cacheEntry.position = 1;
            }
        }

        List<Text> responses = new ArrayList<>();
        responses.add(new Text(String.format(Locale.ENGLISH, "standard response for %s at position %s", field.field(),
                cacheEntry.position)));

        if (field.fieldOptions().options() != null) {
            for (Map.Entry<String, Object> entry : field.fieldOptions().options().entrySet()) {
                responses.add(new Text("field:" + entry.getKey() + ":" + entry.getValue()));
            }
        }

        return new HighlightField(fieldContext.fieldName, responses.toArray(new Text[]{}));
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    private static class CacheEntry {
        private int position;
        private int docId;
    }
}
