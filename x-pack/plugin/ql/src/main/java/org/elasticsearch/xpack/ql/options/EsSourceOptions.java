/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.options;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE;
import static org.elasticsearch.action.support.IndicesOptions.WildcardOptions.ALLOW_NO_INDICES;

/*
 * This provides a repository for index resolution and/or search-time configuration options.
 * Such as: [search] preference and [search / index resolution] allow_no_indices, ignore_unavailable.
 *
 * Some of the options end up in a IndicesOptions instance. However, FieldCaps and Search APIs use IndicesOptions
 * defaults having conflicting values. So this class will just validate and record the user-provided settings first, and then apply these
 * onto a base (an API-specific default).
 */
public class EsSourceOptions {

    private static final String OPTION_PREFERENCE = "preference";
    public static final EsSourceOptions NO_OPTIONS = new EsSourceOptions();

    @Nullable
    private String allowNoIndices;
    @Nullable
    private String ignoreUnavailable;
    @Nullable
    private String preference;

    public EsSourceOptions() {}

    public EsSourceOptions(StreamInput in) throws IOException {
        this.allowNoIndices = in.readOptionalString();
        this.ignoreUnavailable = in.readOptionalString();
        this.preference = in.readOptionalString();
    }

    public IndicesOptions indicesOptions(IndicesOptions base) {
        if (allowNoIndices == null && ignoreUnavailable == null) {
            return base;
        }
        var wildcardOptions = allowNoIndices != null
            ? IndicesOptions.WildcardOptions.parseParameters(null, allowNoIndices, base.wildcardOptions())
            : base.wildcardOptions();
        var targetOptions = ignoreUnavailable != null
            ? IndicesOptions.ConcreteTargetOptions.fromParameter(ignoreUnavailable, base.concreteTargetOptions())
            : base.concreteTargetOptions();
        return new IndicesOptions(targetOptions, wildcardOptions, base.gatekeeperOptions(), base.failureStoreOptions());
    }

    @Nullable
    public String preference() {
        return preference;
    }

    public void addOption(String name, String value) {
        switch (name) {
            case ALLOW_NO_INDICES -> {
                requireUnset(name, allowNoIndices);
                IndicesOptions.WildcardOptions.parseParameters(null, value, null);
                allowNoIndices = value;
            }
            case IGNORE_UNAVAILABLE -> {
                requireUnset(name, ignoreUnavailable);
                IndicesOptions.ConcreteTargetOptions.fromParameter(value, null);
                ignoreUnavailable = value;
            }
            case OPTION_PREFERENCE -> {
                requireUnset(name, preference);
                // The validation applies only for the predefined settings (i.e. prefixed by '_') or empty one (i.e. delegate handling
                // of this case).
                if (value.isEmpty() || value.charAt(0) == '_') {
                    // Note: _search will neither fail, nor warn about something like `preference=_shards:0,1|_doesnotexist`
                    Preference.parse(value);
                }
                preference = value;
            }
            default -> {
                String message = "unknown option named [" + name + "]";
                List<String> matches = StringUtils.findSimilar(name, List.of(ALLOW_NO_INDICES, IGNORE_UNAVAILABLE, OPTION_PREFERENCE));
                if (matches.isEmpty() == false) {
                    String suggestions = matches.size() == 1 ? "[" + matches.get(0) + "]" : "any of " + matches;
                    message += ", did you mean " + suggestions + "?";
                }
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static void requireUnset(String name, String value) {
        if (value != null) {
            throw new IllegalArgumentException("option [" + name + "] has already been provided");
        }
    }

    public void writeEsSourceOptions(StreamOutput out) throws IOException {
        out.writeOptionalString(allowNoIndices);
        out.writeOptionalString(ignoreUnavailable);
        out.writeOptionalString(preference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowNoIndices, ignoreUnavailable, preference);
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
        return Objects.equals(allowNoIndices, other.allowNoIndices)
            && Objects.equals(ignoreUnavailable, other.ignoreUnavailable)
            && Objects.equals(preference, other.preference);
    }
}
