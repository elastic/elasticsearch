/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.wildcard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class WildcardFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numWildcardFields;

    public WildcardFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numWildcardFields = input.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numWildcardFields);
    }

    public WildcardFeatureSetUsage(boolean available,  int numWildcardFields) {
        super(XPackField.WILDCARD, available, true);
        this.numWildcardFields = numWildcardFields;
    }


    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("wildcard_fields_count", numWildcardFields);
    }

    public int numWildcardFields() {
        return numWildcardFields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, numWildcardFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WildcardFeatureSetUsage == false) return false;
        WildcardFeatureSetUsage other = (WildcardFeatureSetUsage) obj;
        return available == other.available && enabled == other.enabled && numWildcardFields == other.numWildcardFields;
    }
}
