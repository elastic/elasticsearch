/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;

public class PostStartBasicAction extends Action<PostStartBasicResponse> {

    public static final PostStartBasicAction INSTANCE = new PostStartBasicAction();
    public static final String NAME = "cluster:admin/xpack/license/start_basic";

    private PostStartBasicAction() {
        super(NAME);
    }

    @Override
    public PostStartBasicResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<PostStartBasicResponse> getResponseReader() {
        return PostStartBasicResponse::new;
    }
}
