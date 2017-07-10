/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class InfoResponse extends Response {

    public final String node, cluster, versionString, versionHash, versionDate;
    public final int majorVersion, minorVersion;

    public InfoResponse(String nodeName, String clusterName, byte versionMajor, byte versionMinor, String version,
            String versionHash, String versionDate) {
        this.node = nodeName;
        this.cluster = clusterName;
        this.versionString = version;
        this.versionHash = versionHash;
        this.versionDate = versionDate;

        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
    }

    InfoResponse(DataInput in) throws IOException {
        node = in.readUTF();
        cluster = in.readUTF();
        majorVersion = in.readByte();
        minorVersion = in.readByte();
        versionString = in.readUTF();
        versionHash = in.readUTF();
        versionDate = in.readUTF();
    }

    @Override
    void write(int clientVersion, DataOutput out) throws IOException {
        out.writeUTF(node);
        out.writeUTF(cluster);
        out.writeByte(majorVersion);
        out.writeByte(minorVersion);
        out.writeUTF(versionString);
        out.writeUTF(versionHash);
        out.writeUTF(versionDate);
    }

    @Override
    protected String toStringBody() {
        return "node=[" + node
                + "] cluster=[" + cluster
                + "] version=[" + versionString
                + "]/[major=[" + majorVersion
                + "] minor=[" + minorVersion
                + "] hash=[" + versionHash
                + "] date=[" + versionDate + "]";
    }

    @Override
    RequestType requestType() {
        return RequestType.INFO;
    }

    @Override
    ResponseType responseType() {
        return ResponseType.INFO;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        InfoResponse other = (InfoResponse) obj;
        return Objects.equals(node, other.node)
                && Objects.equals(cluster, other.cluster)
                && Objects.equals(majorVersion, other.majorVersion)
                && Objects.equals(minorVersion, other.minorVersion)
                && Objects.equals(versionString, other.versionString)
                && Objects.equals(versionHash, other.versionHash)
                && Objects.equals(versionDate, other.versionDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, cluster, majorVersion, minorVersion, versionString, versionHash, versionDate);
    }
}