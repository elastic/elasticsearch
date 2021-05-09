/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.encrypted.keyrotation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryOperation;

import java.io.IOException;
import java.util.List;

public final class KeyRotationsInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final KeyRotationsInProgress EMPTY = new KeyRotationsInProgress(List.of());

    public static final String TYPE = "snapshots";

    private final List<Entry> entries;

    private KeyRotationsInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public static final class Entry implements Writeable, ToXContent, RepositoryOperation {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public String repository() {
            return null;
        }

        @Override
        public long repositoryStateId() {
            return 0;
        }
    }
}
