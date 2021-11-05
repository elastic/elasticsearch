/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response containing a SAML SP metadata for a specific realm as XML.
 */
public class SamlSpMetadataResponse extends ActionResponse {
    public String getXMLString() {
        return XMLString;
    }

    private String XMLString;

    public SamlSpMetadataResponse(StreamInput in) throws IOException {
        super(in);
        XMLString = in.readString();
    }

    public SamlSpMetadataResponse(String XMLString) {
        this.XMLString = XMLString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(XMLString);
    }
}
