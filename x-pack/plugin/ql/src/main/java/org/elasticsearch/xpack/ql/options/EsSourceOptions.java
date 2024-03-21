/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.options;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * This provides a repository for index resolution and/or search-time configuration options.
 * Such as: [search] preference and [search / index resolution] allow_no_indices, ignore_unavailable.
 */
public class EsSourceOptions {

    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
    @Nullable
    private String preference;
    @Nullable
    private List<String> addedOptions;

    public static final EsSourceOptions DEFAULT = new EsSourceOptions();

    public EsSourceOptions() {}

    public EsSourceOptions(StreamInput in) throws IOException {
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.preference = in.readOptionalString();
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Nullable
    public String preference() {
        return preference;
    }

    public void addOption(String name, String value) {
        String loweredName = name.toLowerCase(Locale.ROOT);
        if (addedOptions != null) {
            if (addedOptions.contains(loweredName)) {
                throw new IllegalArgumentException("option [" + name + "] has already been provided");
            }
        } else {
            addedOptions = new ArrayList<>(3); // in sync with switch size
        }
        switch (loweredName) {
            case "allow_no_indices" -> {
                var wildcardOptions = IndicesOptions.WildcardOptions.parseParameters(null, value, indicesOptions.wildcardOptions());
                indicesOptions = new IndicesOptions(
                    indicesOptions.concreteTargetOptions(),
                    wildcardOptions,
                    indicesOptions.gatekeeperOptions(),
                    indicesOptions.failureStoreOptions()
                );
            }
            case "ignore_unavailable" -> {
                var targetOptions = IndicesOptions.ConcreteTargetOptions.fromParameter(value, indicesOptions.concreteTargetOptions());
                indicesOptions = new IndicesOptions(
                    targetOptions,
                    indicesOptions.wildcardOptions(),
                    indicesOptions.gatekeeperOptions(),
                    indicesOptions.failureStoreOptions()
                );
            }
            case "preference" -> {
                // Just a simple validation and only for predefined settings (i.e. prefixed by '_').
                if (value.charAt(0) == '_') {
                    // Note: _search will neither fail, nor warn about something like `preference=_shards:0,1|_doesnotexist`
                    Preference.parse(value);
                }
                preference = value;
            }
            default -> throw new IllegalArgumentException("unknown option named [" + name + "]");
        }
        addedOptions.add(loweredName);
    }

    public void writeEsSourceOptions(StreamOutput out) throws IOException {
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(preference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions, preference);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsSourceOptions other = (EsSourceOptions) obj;
        return Objects.equals(indicesOptions, other.indicesOptions) && Objects.equals(preference, other.preference);
    }
}
