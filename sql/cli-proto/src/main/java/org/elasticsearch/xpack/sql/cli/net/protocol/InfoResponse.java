/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Status;

public class InfoResponse extends Response {

    public final String node, cluster, versionString, versionHash, versionDate;
    public final int majorVersion, minorVersion;

    public InfoResponse(String nodeName, String clusterName, byte versionMajor, byte versionMinor, String version, String versionHash, String versionDate) {
        super(Action.INFO);

        this.node = nodeName;
        this.cluster = clusterName;
        this.versionString = version;
        this.versionHash = versionHash;
        this.versionDate = versionDate;

        this.majorVersion = versionMajor;
        this.minorVersion = versionMinor;
    }

    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toSuccess(action));
        out.writeUTF(node);
        out.writeUTF(cluster);
        out.writeByte(majorVersion);
        out.writeByte(minorVersion);
        out.writeUTF(versionString);
        out.writeUTF(versionHash);
        out.writeUTF(versionDate);
    }

    public static InfoResponse decode(DataInput in) throws IOException {
        String node = in.readUTF();
        String cluster = in.readUTF();
        byte versionMajor = in.readByte();
        byte versionMinor = in.readByte();
        String version = in.readUTF();
        String versionHash = in.readUTF();
        String versionBuild = in.readUTF();

        return new InfoResponse(node, cluster, versionMajor, versionMinor, version, versionHash, versionBuild);
    }
}