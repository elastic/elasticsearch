/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;

public abstract class AbstractRestChannel implements RestChannel {

    protected final RestRequest request;
    private final boolean detailedErrorsEnabled;
    private final String format;
    private final String filterPath;
    private final boolean pretty;
    private final boolean human;
    private final String acceptHeader;

    private BytesStreamOutput bytesOut;

    /**
     * Construct a channel for handling the request.
     *
     * @param request               the request
     * @param detailedErrorsEnabled if detailed errors should be reported to the channel
     * @throws IllegalArgumentException if parsing the pretty or human parameters fails
     */
    protected AbstractRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.format = request.param("format");
        this.acceptHeader = request.header("Accept");
        this.filterPath = request.param("filter_path", null);
        this.pretty = request.paramAsBoolean("pretty", false);
        this.human = request.paramAsBoolean("human", false);
    }

    @Override
    public XContentBuilderFactory xContentBuilderFactory() {
        return new XContentBuilderFactory(request, format, acceptHeader, filterPath, pretty, human, this::bytesOutput);
    }

    /**
     * A channel level bytes output that can be reused. The bytes output is lazily instantiated
     * by a call to {@link #newBytesOutput()}. Once the stream is created, it gets reset on each
     * call to this method.
     */
    @Override
    public final BytesStreamOutput bytesOutput() {
        if (bytesOut == null) {
            bytesOut = newBytesOutput();
        } else {
            bytesOut.reset();
        }
        return bytesOut;
    }

    /**
     * An accessor to the raw value of the channel bytes output. This method will not instantiate
     * a new stream if one does not exist and this method will not reset the stream.
     */
    protected final BytesStreamOutput bytesOutputOrNull() {
        return bytesOut;
    }

    protected BytesStreamOutput newBytesOutput() {
        return new BytesStreamOutput();
    }

    @Override
    public RestRequest request() {
        return this.request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }
}
