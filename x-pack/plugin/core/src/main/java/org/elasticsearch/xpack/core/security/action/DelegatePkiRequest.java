/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.cert.X509Certificate;
import java.util.Arrays;

public class DelegatePkiRequest extends ActionRequest {
    
    private X509Certificate[] certificates;

    public DelegatePkiRequest() { }

    public DelegatePkiRequest(StreamInput in) throws IOException {
        super(in);
        ObjectInputStream ois = new ObjectInputStream(in);
        try {
            this.certificates = (X509Certificate[]) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public X509Certificate[] getCertificates() {
        return certificates;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(certificates);
        oos.flush();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiRequest that = (DelegatePkiRequest) o;
        return Arrays.equals(certificates, that.certificates);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(certificates);
    }
}
