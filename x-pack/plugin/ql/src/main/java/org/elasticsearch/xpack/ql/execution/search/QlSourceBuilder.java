/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@code SqlSourceBuilder} is a builder object passed to objects implementing
 * {@link FieldExtraction} that can "build" whatever needs to be extracted from
 * the resulting ES document as a field.
 */
public class QlSourceBuilder {
    // The LinkedHashMaps preserve the order of the fields in the response
    private final Set<FieldAndFormat> fetchFields = new LinkedHashSet<>();
    private final Map<String, Script> scriptFields = new LinkedHashMap<>();

    boolean trackScores = false;

    public QlSourceBuilder() {}

    /**
     * Turns on returning the {@code _score} for documents.
     */
    public void trackScores() {
        this.trackScores = true;
    }

    /**
     * Retrieve the requested field using the "fields" API
     */
    public void addFetchField(String field, String format) {
        fetchFields.add(new FieldAndFormat(field, format));
    }

    /**
     * Return the given field as a script field with the supplied script
     */
    public void addScriptField(String name, Script script) {
        scriptFields.put(name, script);
    }

    /**
     * Collect the necessary fields, modifying the {@code SearchSourceBuilder}
     * to retrieve them from the document.
     */
    public void build(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.trackScores(this.trackScores);
        fetchFields.forEach(field -> sourceBuilder.fetchField(new FieldAndFormat(field.field, field.format, null)));
        scriptFields.forEach(sourceBuilder::scriptField);
    }
}
