/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to create a new Machine Learning Datafeed given a {@link DatafeedConfig} configuration
 */
public class PutDatafeedRequest implements Validatable, ToXContentObject {

    private final DatafeedConfig datafeed;

    /**
     * Construct a new PutDatafeedRequest
     *
     * @param datafeed a {@link DatafeedConfig} configuration to create
     */
    public PutDatafeedRequest(DatafeedConfig datafeed) {
        this.datafeed = datafeed;
    }

    public DatafeedConfig getDatafeed() {
        return datafeed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return datafeed.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PutDatafeedRequest request = (PutDatafeedRequest) object;
        return Objects.equals(datafeed, request.datafeed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeed);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

}
