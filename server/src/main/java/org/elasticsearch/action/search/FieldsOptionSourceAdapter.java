/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Version;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FieldFetcher;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.transport.Transport.Connection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

interface FieldsOptionSourceAdapter {
    default void adaptRequest(Consumer<SearchSourceBuilder> sourceConsumer) {
        // noop
    };
    default void adaptResponse(SearchHit[] searchHits) {
        // noop
    };

    default boolean getRemoveSourceOnResponse() {
        return false;
    }

    String FIELDS_EMULATION_ERROR_MSG = "Cannot specify both 'fields' and '_source' 'includes' or 'excludes' in"
        + "a search request that is targeting pre version 7.10 nodes.";

    static FieldsOptionSourceAdapter create(Connection connection, SearchSourceBuilder ccsSearchSource) {
        Version version = connection.getVersion();
        if (version.before(Version.V_7_10_0)) {
            List<FieldAndFormat> fetchFields = ccsSearchSource.fetchFields();
            if (fetchFields != null && fetchFields.isEmpty() == false) {
                String[] includes = fetchFields.stream().map(ff -> ff.field).toArray(i -> new String[i]);
                CharacterRunAutomaton unmappedFieldsFetchAutomaton = null;
                // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less space in the
                // lookup automaton
                Map<Boolean, List<String>> partitions = fetchFields.stream()
                    .map(ff -> ff.field)
                    .collect(Collectors.partitioningBy((s -> Regex.isSimpleMatchPattern(s))));
                List<String> unmappedWildcardPattern = partitions.get(true);
                List<String> unmappedConcreteFields = partitions.get(false);
                if (unmappedWildcardPattern.isEmpty() == false) {
                    unmappedFieldsFetchAutomaton = new CharacterRunAutomaton(
                        Regex.simpleMatchToAutomaton(unmappedWildcardPattern.toArray(new String[unmappedWildcardPattern.size()])),
                        100000
                    );
                }
                final FieldFetcher fieldFetcher = new FieldFetcher(
                    Collections.emptyMap(),
                    unmappedFieldsFetchAutomaton,
                    unmappedConcreteFields
                );

                FetchSourceContext fetchSource = ccsSearchSource.fetchSource();
                final boolean removeSourceOnResponse;
                final SearchSourceBuilder adaptedSource;

                if (fetchSource == null || (fetchSource != null && fetchSource.fetchSource())) {
                    // case 1: original request has source: true, but no includes/exclude -> do nothing on request
                    if (fetchSource == null || fetchSource.includes().length == 0 && fetchSource.excludes().length == 0) {
                        // change nothing, we can get everything from source and can leave it when translating the response
                        removeSourceOnResponse = false;
                        adaptedSource = ccsSearchSource;
                    } else {
                        // original request has source includes/excludes set. In this case we don't want to silently
                        // overwrite the source parameter with something else, so we error instead
                        throw new IllegalArgumentException(FIELDS_EMULATION_ERROR_MSG);
                    }
                } else {
                    // case 2: original request has source: false
                    adaptedSource = ccsSearchSource.shallowCopy();
                    adaptedSource.fetchSource(new FetchSourceContext(true));
                    adaptedSource.fetchSource(includes, null);
                    removeSourceOnResponse = true;
                }

                return new FieldsOptionSourceAdapter() {

                    @Override
                    public void adaptRequest(Consumer<SearchSourceBuilder> sourceConsumer) {
                        sourceConsumer.accept(adaptedSource);
                    }

                    @Override
                    public void adaptResponse(SearchHit[] hits) {
                        for (SearchHit hit : hits) {
                            SourceLookup lookup = new SourceLookup();
                            lookup.setSource(hit.getSourceAsMap());
                            Map<String, DocumentField> documentFields = Collections.emptyMap();
                            try {
                                documentFields = fieldFetcher.fetch(lookup);
                            } catch (IOException e) {
                                // best effort fetching field, if this doesn't work continue
                            }
                            for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                                hit.setDocumentField(entry.getKey(), entry.getValue());
                            }
                            if (removeSourceOnResponse) {
                                // original request didn't request source, so we remove it
                                hit.sourceRef(null);
                            }
                        }
                    }

                    @Override
                    public boolean getRemoveSourceOnResponse() {
                        return removeSourceOnResponse;
                    }
                };
            }
        }
        return FieldsOptionSourceAdapter.NOOP_ADAPTER;
    }

    FieldsOptionSourceAdapter NOOP_ADAPTER = new FieldsOptionSourceAdapter() {};
}