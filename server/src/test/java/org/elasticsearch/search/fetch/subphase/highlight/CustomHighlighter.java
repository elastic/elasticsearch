/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        CacheEntry cacheEntry = (CacheEntry) fieldContext.cache.get("test-custom");
        final int docId = fieldContext.hitContext.readerContext().docBase + fieldContext.hitContext.docId();
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            fieldContext.cache.put("test-custom", cacheEntry);
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
        responses.add(
            new Text(String.format(Locale.ENGLISH, "standard response for %s at position %s", field.field(), cacheEntry.position))
        );

        if (field.fieldOptions().options() != null) {
            for (Map.Entry<String, Object> entry : field.fieldOptions().options().entrySet()) {
                responses.add(new Text("field:" + entry.getKey() + ":" + entry.getValue()));
            }
        }

        return new HighlightField(fieldContext.fieldName, responses.toArray(new Text[] {}));
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
