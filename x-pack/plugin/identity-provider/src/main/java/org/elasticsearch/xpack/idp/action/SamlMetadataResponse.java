/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class SamlMetadataResponse extends ActionResponse {

    private final String xmlString;

    public SamlMetadataResponse(String xmlString) {
        this.xmlString = Objects.requireNonNull(xmlString, "Metadata XML string must be provided");
    }

    public String getXmlString() {
        return xmlString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(xmlString);
    }
}
