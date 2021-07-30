/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class ResumeFollowRequest extends FollowConfig implements Validatable, ToXContentObject {

    private final String followerIndex;

    public ResumeFollowRequest(String followerIndex) {
        this.followerIndex = Objects.requireNonNull(followerIndex, "followerIndex");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public String getFollowerIndex() {
        return followerIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ResumeFollowRequest that = (ResumeFollowRequest) o;
        return Objects.equals(followerIndex, that.followerIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), followerIndex);
    }
}
