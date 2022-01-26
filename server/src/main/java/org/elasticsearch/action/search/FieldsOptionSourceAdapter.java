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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class FieldsOptionSourceAdapter {

    static String FIELDS_EMULATION_ERROR_MSG = "Cannot specify both 'fields' and '_source' 'includes' or 'excludes' in"
        + "a search request that is targeting pre version 7.10 nodes.";

    private final SearchSourceBuilder originalSource;
    private final boolean requestShouldBeAdapted;
    private final boolean removeSourceOnResponse;
    private final SearchSourceBuilder adaptedSource;
    private FieldFetcher fieldFetcher;

    FieldsOptionSourceAdapter(SearchRequest request) {
        originalSource = request.source() != null ? request.source() : new SearchSourceBuilder();
        List<FieldAndFormat> fetchFields = originalSource.fetchFields();
        requestShouldBeAdapted = request.isFieldsOptionEmulationEnabled() && fetchFields != null && fetchFields.isEmpty() == false;
        if (requestShouldBeAdapted) {
            FetchSourceContext fetchSource = originalSource.fetchSource();
            if (fetchSource == null || (fetchSource != null && fetchSource.fetchSource())) {
                // case 1: original request has source: true, but no includes/exclude -> do nothing on request
                if (fetchSource == null || fetchSource.includes().length == 0 && fetchSource.excludes().length == 0) {
                    // change nothing, we can get everything from source and can leave it when translating the response
                    removeSourceOnResponse = false;
                    adaptedSource = originalSource;
                } else {
                    // original request has source includes/excludes set. In this case we don't want to silently
                    // overwrite the source parameter with something else, so we error instead
                    throw new IllegalArgumentException(FIELDS_EMULATION_ERROR_MSG);
                }
            } else {
                // case 2: original request has source: false
                adaptedSource = originalSource.shallowCopy();
                adaptedSource.fetchSource(new FetchSourceContext(true));
                String[] includes = fetchFields.stream().map(ff -> ff.field).toArray(i -> new String[i]);
                adaptedSource.fetchSource(includes, null);
                removeSourceOnResponse = true;
            }

        } else {
            removeSourceOnResponse = false;
            adaptedSource = null;
        }
    }

    /**
     * Swaps the existing search source with one that has "source" fetching enabled and configured so that
     * we retrieve the fields requested by the "fields" option in the original request.
     * This is only done for connections to pre-7.10 nodes and if the fields emulation has been enabled, otherwise
     * calling this method will be a no-op.
     */
    public void adaptRequest(Version connectionVersion, Consumer<SearchSourceBuilder> sourceConsumer) {
        if (requestShouldBeAdapted && connectionVersion.before(Version.V_7_10_0) && adaptedSource != null) {
            sourceConsumer.accept(adaptedSource);
        }
    }

    /**
     * Goes through all hits in the response and fetches fields requested by the "fields" option in the original request by
     * fetching them from source. If the original request has "source" disabled, this method will also delete the
     * source section in the hit.
     * This is only done for connections to pre-7.10 nodes and if the fields emulation has been enabled, otherwise
     * calling this method will be a no-op.
     */
    public void adaptResponse(Version connectionVersion, SearchHit[] hits) {
        if (requestShouldBeAdapted && connectionVersion.before(Version.V_7_10_0)) {
            for (SearchHit hit : hits) {
                SourceLookup lookup = new SourceLookup();
                lookup.setSource(hit.getSourceAsMap());
                Map<String, DocumentField> documentFields = Collections.emptyMap();
                try {
                    if (fieldFetcher == null) {
                        CharacterRunAutomaton unmappedFieldsFetchAutomaton = null;
                        // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less space in the
                        // lookup automaton
                        Map<Boolean, List<String>> partitions = originalSource.fetchFields()
                            .stream()
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
                        fieldFetcher = new FieldFetcher(Collections.emptyMap(), unmappedFieldsFetchAutomaton, unmappedConcreteFields);

                    }
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
    }

    boolean getRemoveSourceOnResponse() {
        return removeSourceOnResponse;
    }
}
