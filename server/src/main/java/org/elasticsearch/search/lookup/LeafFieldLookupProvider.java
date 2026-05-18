/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Defines how to populate the values of a {@link FieldLookup}
 */
public interface LeafFieldLookupProvider {

    /**
     * Load stored field values for the given doc and cache them in the FieldLookup
     */
    void populateFieldLookup(FieldLookup fieldLookup, int doc) throws IOException;

    /**
     * Create a LeafFieldLookupProvider that loads values from stored fields
     */
    static Function<LeafReaderContext, LeafFieldLookupProvider> fromStoredFields() {
        return ctx -> new LeafFieldLookupProvider() {

            StoredFields storedFields;

            @Override
            public void populateFieldLookup(FieldLookup fieldLookup, int doc) throws IOException {
                if (storedFields == null) {
                    storedFields = ctx.reader().storedFields();
                }
                // TODO can we remember which fields have been loaded here and get them eagerly next time?
                // likelihood is if a script is loading several fields on one doc they will load the same
                // set of fields next time round
                final List<Object> currentValues = new ArrayList<>(2);
                storedFields.document(doc, new SingleFieldsVisitor(fieldLookup.fieldType(), currentValues));
                fieldLookup.setValues(currentValues);
            }

        };
    }

}
