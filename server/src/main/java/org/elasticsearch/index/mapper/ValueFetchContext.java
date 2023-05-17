/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.Version;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Set;
import java.util.function.Function;

public interface ValueFetchContext {

    boolean isSourceEnabled();

    Set<String> sourcePath(String fieldName);

    SearchLookup lookup();

    boolean containsBrokenAnalysis(String field);

    MappedFieldType getFieldType(String field);

    NestedLookup nestedLookup();

    Set<String> getMatchingFieldNames(String fieldPattern);

    boolean isMetadataField(String field);

    Version indexVersionCreated();

    IndexSettings getIndexSettings();

    Analyzer getIndexAnalyzer(Function<String, NamedAnalyzer> unindexedFieldAnalyzer);

    <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType mappedFieldType, MappedFieldType.FielddataOperation search);

    Index getFullyQualifiedIndex();

    boolean allowExpensiveQueries();
}
