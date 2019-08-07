/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@code SqlSourceBuilder} is a builder object passed to objects implementing
 * {@link FieldExtraction} that can "build" whatever needs to be extracted from
 * the resulting ES document as a field.
 */
public class SqlSourceBuilder {
    // The LinkedHashMaps preserve the order of the fields in the response
    final Set<String> sourceFields = new LinkedHashSet<>();
    final Set<FieldAndFormat> docFields = new LinkedHashSet<>();
    final Map<String, Script> scriptFields = new LinkedHashMap<>();

    boolean trackScores = false;

    public SqlSourceBuilder() {
    }

    /**
     * Turns on returning the {@code _score} for documents.
     */
    public void trackScores() {
        this.trackScores = true;
    }

    /**
     * Retrieve the requested field from the {@code _source} of the document
     */
    public void addSourceField(String field) {
        sourceFields.add(field);
    }

    /**
     * Retrieve the requested field from doc values (or fielddata) of the document
     */
    public void addDocField(String field, String format) {
        docFields.add(new FieldAndFormat(field, format));
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
        if (!sourceFields.isEmpty()) {
            sourceBuilder.fetchSource(sourceFields.toArray(Strings.EMPTY_ARRAY), null);
        }
        docFields.forEach(field -> sourceBuilder.docValueField(field.field, field.format));
        scriptFields.forEach(sourceBuilder::scriptField);
    }
}
