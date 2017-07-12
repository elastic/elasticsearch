/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * General information about the server.
 */
public abstract class AbstractInfoResponse extends Response {
    public final String node, cluster, versionString, versionHash, versionDate;
    public final int majorVersion, minorVersion;

    protected AbstractInfoResponse(String nodeName, String clusterName, byte versionMajor, byte versionMinor, String version,
            String versionHash, String versionDate) {
        this.node = nodeName;
        this.cluster = clusterName;
        this.versionString = version;
        this.versionHash = versionHash;
        this.versionDate = versionDate;

        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
    }

    protected AbstractInfoResponse(Request request, DataInput in) throws IOException {
        node = in.readUTF();
        cluster = in.readUTF();
        majorVersion = in.readByte();
        minorVersion = in.readByte();
        versionString = in.readUTF();
        versionHash = in.readUTF();
        versionDate = in.readUTF();
    }

    @Override
    protected final void write(int clientVersion, DataOutput out) throws IOException {
        out.writeUTF(node);
        out.writeUTF(cluster);
        out.writeByte(majorVersion);
        out.writeByte(minorVersion);
        out.writeUTF(versionString);
        out.writeUTF(versionHash);
        out.writeUTF(versionDate);
    }

    @Override
    protected final String toStringBody() {
        return "node=[" + node
                + "] cluster=[" + cluster
                + "] version=[" + versionString
                + "]/[major=[" + majorVersion
                + "] minor=[" + minorVersion
                + "] hash=[" + versionHash
                + "] date=[" + versionDate + "]]";
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractInfoResponse other = (AbstractInfoResponse) obj;
        return Objects.equals(node, other.node)
                && Objects.equals(cluster, other.cluster)
                && Objects.equals(majorVersion, other.majorVersion)
                && Objects.equals(minorVersion, other.minorVersion)
                && Objects.equals(versionString, other.versionString)
                && Objects.equals(versionHash, other.versionHash)
                && Objects.equals(versionDate, other.versionDate);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(node, cluster, majorVersion, minorVersion, versionString, versionHash, versionDate);
    }
}