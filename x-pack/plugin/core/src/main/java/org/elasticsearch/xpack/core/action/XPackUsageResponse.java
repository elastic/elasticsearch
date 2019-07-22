/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XPackUsageResponse extends ActionResponse {

    private List<XPackFeatureSet.Usage> usages;

    public XPackUsageResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        usages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            usages.add(in.readNamedWriteable(XPackFeatureSet.Usage.class));
        }
    }

    public XPackUsageResponse(List<XPackFeatureSet.Usage> usages) {
        this.usages = usages;
    }

    public List<XPackFeatureSet.Usage> getUsages() {
        return usages;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(usages.size());
        for (XPackFeatureSet.Usage usage : usages) {
            out.writeNamedWriteable(usage);
        }
    }

    }
