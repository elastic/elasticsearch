/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.util.ArrayUtils;

/**
 * A request to retrieve information about an index.
 */
public class GetIndexRequest extends TimedRequest {

    public enum Feature {
        ALIASES,
        MAPPINGS,
        SETTINGS;
    }

    static final Feature[] DEFAULT_FEATURES = new Feature[] { Feature.ALIASES, Feature.MAPPINGS, Feature.SETTINGS };
    private Feature[] features = DEFAULT_FEATURES;
    private boolean humanReadable = false;
    private transient boolean includeDefaults = false;

    private final String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);
    private boolean local = false;

    public GetIndexRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * The indices into which the mappings will be put.
     */
    public String[] indices() {
        return indices;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public GetIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public final GetIndexRequest local(boolean local) {
        this.local = local;
        return this;
    }

    /**
     * Return local information, do not retrieve the state from master node (default: false).
     * @return <code>true</code> if local information is to be returned;
     * <code>false</code> if information is to be retrieved from master node (default).
     */
    public final boolean local() {
        return local;
    }

    public GetIndexRequest features(Feature... features) {
        if (features == null) {
            throw new IllegalArgumentException("features cannot be null");
        } else {
            this.features = features;
        }
        return this;
    }

    public GetIndexRequest addFeatures(Feature... features) {
        if (this.features == DEFAULT_FEATURES) {
            return features(features);
        } else {
            return features(ArrayUtils.concat(features(), features, Feature.class));
        }
    }

    public Feature[] features() {
        return features;
    }

    public GetIndexRequest humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    public boolean humanReadable() {
        return humanReadable;
    }

    /**
     * Sets the value of "include_defaults".
     *
     * @param includeDefaults value of "include_defaults" to be set.
     * @return this request
     */
    public GetIndexRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    /**
     * Whether to return all default settings for each of the indices.
     *
     * @return <code>true</code> if defaults settings for each of the indices need to returned;
     * <code>false</code> otherwise.
     */
    public boolean includeDefaults() {
        return includeDefaults;
    }


}
