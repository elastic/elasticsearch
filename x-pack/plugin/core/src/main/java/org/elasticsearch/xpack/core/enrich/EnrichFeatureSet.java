/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;

public class EnrichFeatureSet implements XPackFeatureSet {

    @Override
    public String name() {
        return XPackField.ENRICH;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        listener.onResponse(new Usage());
    }

    public static class Usage extends XPackFeatureSet.Usage {

        Usage() {
            super(XPackField.ENRICH, true, true);
        }

        public Usage(StreamInput input) throws IOException {
            super(input);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_7_5_0;
        }

    }
}
