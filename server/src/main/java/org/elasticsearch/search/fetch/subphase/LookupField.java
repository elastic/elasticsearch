/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds a specification and resolved values (if any) of {@link org.elasticsearch.index.mapper.LookupRuntimeFieldType} for a search hit.
 * Data nodes create unresolved {@link LookupField} for each search hit during the fetch phase and coordinating nodes resolve them using
 * {@link FetchLookupFieldsPhase}.
 */
public final class LookupField implements Writeable {
    // Lookup request
    private final String lookupIndex;
    private final String lookupId;
    private final List<String> lookupFields;

    // Resolved values
    private List<DocumentField> values;
    private Exception failure;

    LookupField(String lookupIndex, String lookupId, List<String> lookupFields) {
        this.lookupIndex = Objects.requireNonNull(lookupIndex);
        this.lookupId = Objects.requireNonNull(lookupId);
        this.lookupFields = Objects.requireNonNull(lookupFields);
    }

    public LookupField(StreamInput in) throws IOException {
        this.lookupIndex = in.readString();
        this.lookupId = in.readString();
        this.lookupFields = in.readStringList();
        if (in.readBoolean()) {
            this.values = in.readList(DocumentField::new);
        }
        this.failure = in.readException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(lookupIndex);
        out.writeString(lookupId);
        out.writeStringCollection(lookupFields);
        if (values != null) {
            out.writeBoolean(true);
            out.writeList(values);
        } else {
            out.writeBoolean(false);
        }
        out.writeException(failure);
    }

    public boolean isResolved() {
        return values != null || failure != null;
    }

    void setValues(List<DocumentField> values) {
        assert isResolved() == false : "already resolved";
        this.values = Objects.requireNonNull(values);
    }

    void setFailure(Exception failure) {
        assert isResolved() == false : "already resolved";
        this.failure = Objects.requireNonNull(failure);
    }

    List<DocumentField> getValues() {
        return values;
    }

    Exception getFailure() {
        return failure;
    }

    String getLookupIndex() {
        return lookupIndex;
    }

    String getLookupId() {
        return lookupId;
    }

    List<String> getLookupFields() {
        return lookupFields;
    }

    /**
     * Convert the resolved value or failure to a single value of a {@link DocumentField}
     */
    public Object asDocumentFieldValue() {
        assert isResolved() : "unresolved lookup field";
        final Map<String, Object> docFields = new HashMap<>();
        docFields.put("_id", List.of(lookupId));
        if (failure != null) {
            docFields.put(
                "_failure",
                List.of(
                    Map.of(
                        "type",
                        ElasticsearchException.getExceptionName(failure),
                        "status",
                        ExceptionsHelper.status(failure).name(),
                        "reason",
                        failure.getMessage()
                    )
                )
            );
        } else {
            for (DocumentField doc : values) {
                docFields.put(doc.getName(), doc.getValues());
            }
        }
        return docFields;
    }
}
